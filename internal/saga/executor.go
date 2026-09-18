package saga

import (
	"context"
	"fmt"
	"time"
)

// Work performs one remote side effect and returns its observed effect.
type Work func() (*Effect, error)

// PostConditionBuilder derives the verifiable postcondition from an effect.
type PostConditionBuilder func(*Effect) PostCondition

// Reconciler queries the backend to resolve an unknown step before retry.
type Reconciler interface {
	Reconcile(ctx context.Context, req ReconcileRequest) (Decision, error)
}

// ReconcileRequest carries everything needed to recognize a remote effect.
type ReconcileRequest struct {
	Kind        StepKind
	InputDigest string
	Post        *PostCondition
	Window      TimeWindow
	// KnownRemoteIDs are effects already verified for sibling steps in the same
	// operation, so they can be excluded from candidate matching.
	KnownRemoteIDs []string
}

// TimeWindow bounds where in time the remote effect could have been created.
type TimeWindow struct {
	Start time.Time
	End   time.Time
}

// DecisionOutcome enumerates reconciliation results.
type DecisionOutcome string

const (
	// DecisionVerified: the remote effect exists and must be adopted.
	DecisionVerified DecisionOutcome = "verified"
	// DecisionAbsent: the remote effect was confirmed not to exist; retry is safe.
	DecisionAbsent DecisionOutcome = "absent"
	// DecisionUnknown: backend state cannot resolve the step.
	DecisionUnknown DecisionOutcome = "unknown"
)

// Candidate describes a possible remote match for the manual checklist.
type Candidate struct {
	RemoteID  string    `json:"remote_id"`
	RemoteURL string    `json:"remote_url,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// Decision is a reconciler verdict.
type Decision struct {
	Outcome      DecisionOutcome
	Method       string
	Reason       string
	Effect       *Effect
	Compensation *Compensation
	Post         *PostCondition
	Candidates   []Candidate
}

// Reconcile outcome audit constants.
const (
	reconcileVerified = "verified"
	reconcileAbsent   = "absent"
	reconcileUnknown  = "unknown"
	reconcileError    = "error"
)

const (
	// Margin before/after the dispatched attempt used when scanning backend time
	// ranges. Clock skew and backend indexing delay are accounted for.
	defaultWindowBefore = 60 * time.Second
	defaultWindowAfter  = 180 * time.Second
)

// Executor runs idempotent saga steps against an append-only journal.
type Executor struct {
	store        *Store
	journal      *Journal
	recon        Reconciler
	clock        func() time.Time
	windowBefore time.Duration
	windowAfter  time.Duration
}

// NewExecutor wraps an opened journal with execution semantics.
func NewExecutor(store *Store, journal *Journal, recon Reconciler) *Executor {
	return &Executor{
		store:        store,
		journal:      journal,
		recon:        recon,
		clock:        time.Now,
		windowBefore: defaultWindowBefore,
		windowAfter:  defaultWindowAfter,
	}
}

// WithClock overrides the clock (tests).
func (e *Executor) WithClock(clock func() time.Time) *Executor {
	if clock != nil {
		e.clock = clock
	}
	return e
}

// Journal exposes the underlying journal.
func (e *Executor) Journal() *Journal { return e.journal }

// Op exposes the operation state.
func (e *Executor) Op() *Operation { return e.journal.Op() }

// RunStep executes one idempotent step: succeeded steps are served from the
// journal, unknown steps are reconciled before any retry, and pending/failed
// steps are executed exactly once per invocation.
func (e *Executor) RunStep(ctx context.Context, id string, kind StepKind, inputDigest, compensationKind string, buildPost PostConditionBuilder, work Work) (*Effect, error) {
	step, err := e.journal.PlanStep(id, kind, inputDigest)
	if err != nil {
		return nil, err
	}

	if e.Op().IsTerminalCompleted() {
		if step.Status == StepStatusSucceeded && step.Effect != nil {
			return step.Effect, nil
		}
		return nil, fmt.Errorf("saga operation %s is already completed", e.Op().ID)
	}

	switch step.Status {
	case StepStatusSucceeded:
		if step.Effect == nil {
			return nil, fmt.Errorf("saga step %s is succeeded without a recorded effect", id)
		}
		_ = e.journal.MarkReused(id)
		return step.Effect, nil
	case StepStatusUnknown:
		decision, err := e.resolveUnknown(ctx, step)
		if err != nil {
			return nil, err
		}
		switch decision.Outcome {
		case DecisionVerified:
			if decision.Effect == nil {
				return nil, fmt.Errorf("reconciler verified step %s without an effect", id)
			}
			post := PostCondition{}
			comp := Compensation{Kind: compensationKind, RemoteID: decision.Effect.RemoteID}
			if decision.Post != nil {
				post = *decision.Post
			}
			if decision.Compensation != nil {
				comp = *decision.Compensation
			}
			method := decision.Method
			if method == "" {
				method = VerifiedByRemoteID
			}
			if err := e.journal.StepVerified(id, method, *decision.Effect, comp, post); err != nil {
				return nil, err
			}
			return decision.Effect, nil
		case DecisionAbsent:
			if err := e.journal.StepReset(id, decision.Method); err != nil {
				return nil, err
			}
			// Fall through to execute.
		default:
			return nil, &AmbiguousStepError{
				OperationID: e.Op().ID,
				StepID:      id,
				Reason:      decision.Reason,
			}
		}
	}

	return e.execute(id, compensationKind, buildPost, work)
}

// ReconcileUnknown scans every unknown step and records verdicts without
// performing new side effects. It is the body of `saga reconcile`.
func (e *Executor) ReconcileUnknown(ctx context.Context) (map[string]Decision, error) {
	results := map[string]Decision{}
	for _, id := range append([]string(nil), e.Op().Order...) {
		step := e.Op().Steps[id]
		if step == nil || step.Status != StepStatusUnknown {
			continue
		}
		decision, err := e.resolveUnknown(ctx, step)
		if err != nil {
			return results, err
		}
		results[id] = decision
		switch decision.Outcome {
		case DecisionVerified:
			if decision.Effect == nil {
				continue
			}
			post := PostCondition{}
			comp := Compensation{}
			if decision.Post != nil {
				post = *decision.Post
			}
			if decision.Compensation != nil {
				comp = *decision.Compensation
			}
			method := decision.Method
			if method == "" {
				method = VerifiedByRemoteID
			}
			if err := e.journal.StepVerified(id, method, *decision.Effect, comp, post); err != nil {
				return results, err
			}
		case DecisionAbsent:
			if err := e.journal.StepReset(id, decision.Method); err != nil {
				return results, err
			}
		}
	}
	return results, nil
}

func (e *Executor) resolveUnknown(ctx context.Context, step *Step) (Decision, error) {
	if e.recon == nil {
		decision := Decision{Outcome: DecisionUnknown, Reason: "no reconciler configured"}
		_ = e.recordReconcile(step.ID, reconcileUnknown, "", decision)
		return decision, nil
	}
	window := e.attemptWindow(step)
	decision, err := e.recon.Reconcile(ctx, ReconcileRequest{
		Kind:           step.Kind,
		InputDigest:    step.InputDigest,
		Post:           step.PostCondition,
		Window:         window,
		KnownRemoteIDs: e.knownRemoteIDs(step.ID),
	})
	if err != nil {
		_ = e.recordReconcile(step.ID, reconcileError, "", Decision{Outcome: DecisionUnknown, Reason: err.Error()})
		return Decision{Outcome: DecisionUnknown, Reason: err.Error()}, nil
	}
	method := ""
	switch decision.Outcome {
	case DecisionVerified:
		method = reconcileVerified
	case DecisionAbsent:
		method = reconcileAbsent
	default:
		method = reconcileUnknown
	}
	_ = e.recordReconcile(step.ID, method, decision.Method, decision)
	return decision, nil
}

func (e *Executor) recordReconcile(stepID, outcome, method string, decision Decision) error {
	return e.journal.ReconcileResult(stepID, outcome, method, decision.Reason, decision.Candidates)
}

func (e *Executor) knownRemoteIDs(excludeStep string) []string {
	var ids []string
	for _, id := range e.Op().Order {
		if id == excludeStep {
			continue
		}
		step := e.Op().Steps[id]
		if step != nil && step.Effect != nil && step.Effect.RemoteID != "" {
			ids = append(ids, step.Effect.RemoteID)
		}
	}
	return ids
}

func (e *Executor) attemptWindow(step *Step) TimeWindow {
	end := e.clock()
	start := end.Add(-e.windowAfter)
	if last := step.LastAttempt(); last != nil {
		if !last.StartedAt.IsZero() {
			start = last.StartedAt.Add(-e.windowBefore)
		}
		if !last.EndedAt.IsZero() {
			end = last.EndedAt.Add(e.windowAfter)
		} else {
			end = last.StartedAt.Add(e.windowAfter)
		}
	}
	return TimeWindow{Start: start, End: end}
}

func (e *Executor) execute(id, compensationKind string, buildPost PostConditionBuilder, work Work) (*Effect, error) {
	attempt, err := e.journal.AttemptStarted(id)
	if err != nil {
		return nil, err
	}
	effect, workErr := work()
	if workErr != nil {
		ambiguous := IsAmbiguousError(workErr)
		_ = e.journal.AttemptFailed(id, attempt, workErr, ambiguous)
		return nil, workErr
	}
	if effect == nil || effect.RemoteID == "" {
		emptyErr := fmt.Errorf("side effect returned without a remote identifier")
		_ = e.journal.AttemptFailed(id, attempt, emptyErr, false)
		return nil, emptyErr
	}
	compensation := Compensation{Kind: compensationKind, RemoteID: effect.RemoteID}
	post := PostCondition{}
	if buildPost != nil {
		post = buildPost(effect)
	}
	if post.Kind == "" {
		post.Kind = string(e.Op().Steps[id].Kind)
	}
	if post.RemoteID == "" {
		post.RemoteID = effect.RemoteID
	}
	if post.RemoteURL == "" {
		post.RemoteURL = effect.RemoteURL
	}
	if post.ContentDigest == "" {
		post.ContentDigest = e.Op().Steps[id].InputDigest
	}
	if err := e.journal.StepEffect(id, attempt, *effect, compensation, post); err != nil {
		return nil, err
	}
	return effect, nil
}

// Finish records the terminal operation status derived from the journal.
func (e *Executor) Finish() (OpStatus, error) {
	status := DeriveStatus(e.Op())
	if err := e.journal.Terminal(status); err != nil {
		return "", err
	}
	return status, nil
}

// Report builds the observable report for the current operation.
func (e *Executor) Report() *Report {
	return BuildReport(e.Op(), e.journal.Path())
}
