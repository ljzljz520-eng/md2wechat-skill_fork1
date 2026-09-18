package saga

import (
	"context"
	"errors"
	"testing"
)

type fakeReconciler struct {
	decision Decision
	err      error
	requests []ReconcileRequest
}

func (f *fakeReconciler) Reconcile(_ context.Context, req ReconcileRequest) (Decision, error) {
	f.requests = append(f.requests, req)
	return f.decision, f.err
}

func newTestExecutor(t *testing.T, opID string, recon Reconciler) (*Store, *Executor) {
	t.Helper()
	store := newTestStore(t)
	j := beginTestOp(t, store, opID, "input-"+opID, fixedClock())
	t.Cleanup(func() { _ = j.Close() })
	return store, NewExecutor(store, j, recon)
}

func successWork(calls *int, id string) Work {
	return func() (*Effect, error) {
		*calls++
		return &Effect{RemoteID: id, RemoteURL: "https://example/" + id}, nil
	}
}

func simplePost(id string) PostConditionBuilder {
	return func(e *Effect) PostCondition {
		return PostCondition{Kind: "wechat_permanent_image", RemoteID: e.RemoteID}
	}
}

func TestExecutorSucceededStepSkipsWork(t *testing.T) {
	recon := &fakeReconciler{}
	_, exec := newTestExecutor(t, "pub-skip", recon)
	calls := 0

	for i := 0; i < 2; i++ {
		effect, err := exec.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
			CompensationDeleteMaterial, simplePost("r1"), successWork(&calls, "r1"))
		if err != nil {
			t.Fatalf("run %d: %v", i, err)
		}
		if effect.RemoteID != "r1" {
			t.Fatalf("effect = %#v", effect)
		}
	}
	if calls != 1 {
		t.Fatalf("work called %d times, want 1 (second run must be served from journal)", calls)
	}
	if len(recon.requests) != 0 {
		t.Fatalf("reconciler called %d times for a succeeded step", len(recon.requests))
	}
}

func TestExecutorPendingFailureRetriesAcrossProcess(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-proc", "input", clock)
	exec1 := NewExecutor(store, j, &fakeReconciler{decision: Decision{Outcome: DecisionUnknown}})
	calls := 0
	_, err := exec1.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
		CompensationDeleteMaterial, simplePost("r1"), func() (*Effect, error) {
			calls++
			return nil, errors.New("400 bad request")
		})
	if err == nil {
		t.Fatal("deterministic failure returned nil")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a new process: replay + new executor.
	j2, err := store.Resume("pub-proc", OpKindPublish, "input", clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j2.Close() }()
	exec2 := NewExecutor(store, j2, nil)
	effect, err := exec2.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
		CompensationDeleteMaterial, simplePost("r2"), successWork(&calls, "r2"))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if effect.RemoteID != "r2" || calls != 2 {
		t.Fatalf("effect=%#v calls=%d", effect, calls)
	}
	if status := j2.Op().Steps["asset:0"].Status; status != StepStatusSucceeded {
		t.Fatalf("status = %s", status)
	}
}

func TestExecutorUnknownReconcileMatrix(t *testing.T) {
	tests := []struct {
		name       string
		outcome    DecisionOutcome
		wantError  bool
		wantCalls  int
		wantStatus StepStatus
	}{
		{"verified adopts remote effect", DecisionVerified, false, 0, StepStatusSucceeded},
		{"absent resets and executes once", DecisionAbsent, false, 1, StepStatusSucceeded},
		{"unknown blocks retry", DecisionUnknown, true, 0, StepStatusUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t)
			clock := fixedClock()
			j := beginTestOp(t, store, "pub-"+string(tc.outcome), "input", clock)
			recon := &fakeReconciler{decision: Decision{
				Outcome:      tc.outcome,
				Method:       VerifiedByWindowUnique,
				Reason:       "test decision",
				Effect:       &Effect{RemoteID: "remote-found", RemoteURL: "https://example/found"},
				Compensation: &Compensation{Kind: CompensationDeleteMaterial, RemoteID: "remote-found"},
				Post:         &PostCondition{Kind: "wechat_permanent_image", RemoteID: "remote-found"},
			}}
			exec := NewExecutor(store, j, recon)

			// First attempt fails ambiguously: request may have landed remotely.
			_, runErr := exec.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
				CompensationDeleteMaterial, simplePost("r1"), func() (*Effect, error) {
					return nil, errors.New("read tcp: connection reset by peer")
				})
			if runErr == nil {
				t.Fatal("ambiguous failure returned nil")
			}

			calls := 0
			effect, err := exec.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
				CompensationDeleteMaterial, simplePost("r1"), successWork(&calls, "r1"))
			if tc.wantError {
				var ambErr *AmbiguousStepError
				if !errors.As(err, &ambErr) {
					t.Fatalf("err = %v, want AmbiguousStepError", err)
				}
			} else if err != nil {
				t.Fatalf("second run: %v", err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("work calls = %d, want %d", calls, tc.wantCalls)
			}
			if got := j.Op().Steps["asset:0"].Status; got != tc.wantStatus {
				t.Fatalf("status = %s, want %s", got, tc.wantStatus)
			}
			if tc.outcome == DecisionVerified && (effect == nil || effect.RemoteID != "remote-found") {
				t.Fatalf("verified effect = %#v", effect)
			}
			if len(recon.requests) != 1 {
				t.Fatalf("reconcile requests = %d", len(recon.requests))
			}
		})
	}
}

func TestExecutorAmbiguousErrorClassifiedAsUnknown(t *testing.T) {
	_, exec := newTestExecutor(t, "pub-classify", nil)
	_, err := exec.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
		CompensationDeleteMaterial, simplePost("r"), func() (*Effect, error) {
			return nil, deadlineErr{}
		})
	if err == nil {
		t.Fatal("expected error")
	}
	if exec.Op().Steps["asset:0"].Status != StepStatusUnknown {
		t.Fatalf("status = %s, want unknown", exec.Op().Steps["asset:0"].Status)
	}
}

type deadlineErr struct{}

func (deadlineErr) Error() string   { return "context deadline exceeded" }
func (deadlineErr) Timeout() bool   { return true }
func (deadlineErr) Temporary() bool { return true }

func TestExecutorReconcileErrorStaysUnknown(t *testing.T) {
	store := newTestStore(t)
	j := beginTestOp(t, store, "pub-reconerr", "input", fixedClock())
	recon := &fakeReconciler{err: errors.New("backend unavailable")}
	exec := NewExecutor(store, j, recon)
	if _, err := j.PlanStep("asset:0", StepKindUploadMaterial, "d"); err != nil {
		t.Fatal(err)
	}
	attempt, _ := j.AttemptStarted("asset:0")
	if err := j.AttemptFailed("asset:0", attempt, errors.New("timeout"), true); err != nil {
		t.Fatal(err)
	}
	_, err := exec.RunStep(context.Background(), "asset:0", StepKindUploadMaterial, "d",
		CompensationDeleteMaterial, simplePost("r"), func() (*Effect, error) {
			t.Fatal("work must not run while unknown and reconciliation errored")
			return nil, nil
		})
	var ambErr *AmbiguousStepError
	if !errors.As(err, &ambErr) {
		t.Fatalf("err = %v, want AmbiguousStepError", err)
	}
}

func TestExecutorReconcileUnknownProcessesAllUnknownSteps(t *testing.T) {
	store := newTestStore(t)
	j := beginTestOp(t, store, "pub-reconall", "input", fixedClock())
	recon := &fakeReconciler{decision: Decision{
		Outcome:      DecisionVerified,
		Method:       VerifiedByContentFingerprint,
		Effect:       &Effect{RemoteID: "found"},
		Post:         &PostCondition{Kind: "wechat_draft", RemoteID: "found"},
		Compensation: &Compensation{Kind: CompensationDeleteDraft, RemoteID: "found"},
	}}
	exec := NewExecutor(store, j, recon)
	for _, id := range []string{"asset:0", "asset:1"} {
		if _, err := j.PlanStep(id, StepKindUploadMaterial, "d"); err != nil {
			t.Fatal(err)
		}
		attempt, _ := j.AttemptStarted(id)
		if err := j.AttemptFailed(id, attempt, errors.New("timeout"), true); err != nil {
			t.Fatal(err)
		}
	}
	results, err := exec.ReconcileUnknown(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d", len(results))
	}
	for _, id := range []string{"asset:0", "asset:1"} {
		if j.Op().Steps[id].Status != StepStatusSucceeded || j.Op().Steps[id].Effect.RemoteID != "found" {
			t.Fatalf("step %s = %#v", id, j.Op().Steps[id])
		}
	}
}

func TestExecutorExcludesSiblingEffectsFromReconcile(t *testing.T) {
	store := newTestStore(t)
	j := beginTestOp(t, store, "pub-siblings", "input", fixedClock())
	recon := &fakeReconciler{}
	exec := NewExecutor(store, j, recon)

	// asset:0 is already verified with a known remote id.
	if _, err := j.PlanStep("asset:0", StepKindUploadMaterial, "d0"); err != nil {
		t.Fatal(err)
	}
	a0, _ := j.AttemptStarted("asset:0")
	if err := j.StepEffect("asset:0", a0, Effect{RemoteID: "sibling-id"},
		Compensation{Kind: CompensationDeleteMaterial, RemoteID: "sibling-id"},
		PostCondition{Kind: "k", RemoteID: "sibling-id"}); err != nil {
		t.Fatal(err)
	}

	if _, err := j.PlanStep("asset:1", StepKindUploadMaterial, "d1"); err != nil {
		t.Fatal(err)
	}
	a1, _ := j.AttemptStarted("asset:1")
	if err := j.AttemptFailed("asset:1", a1, errors.New("timeout"), true); err != nil {
		t.Fatal(err)
	}

	_, err := exec.RunStep(context.Background(), "asset:1", StepKindUploadMaterial, "d1",
		CompensationDeleteMaterial, simplePost("r"), func() (*Effect, error) {
			t.Fatal("work must not run")
			return nil, nil
		})
	var ambErr *AmbiguousStepError
	if !errors.As(err, &ambErr) {
		t.Fatalf("err = %v", err)
	}
	if len(recon.requests) != 1 {
		t.Fatalf("reconcile requests = %d", len(recon.requests))
	}
	ids := recon.requests[0].KnownRemoteIDs
	if len(ids) != 1 || ids[0] != "sibling-id" {
		t.Fatalf("known remote ids = %#v", ids)
	}
}

func TestExecutorFinishDerivesStatus(t *testing.T) {
	tests := []struct {
		name  string
		steps map[string]StepStatus
		want  OpStatus
	}{
		{"no steps", map[string]StepStatus{}, OpStatusPartial},
		{"all succeeded", map[string]StepStatus{"a": StepStatusSucceeded, "b": StepStatusSucceeded}, OpStatusCompleted},
		{"one failed", map[string]StepStatus{"a": StepStatusSucceeded, "b": StepStatusFailed}, OpStatusPartial},
		{"one pending", map[string]StepStatus{"a": StepStatusSucceeded, "b": StepStatusPending}, OpStatusPartial},
		{"one unknown", map[string]StepStatus{"a": StepStatusSucceeded, "b": StepStatusUnknown}, OpStatusUnknown},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveStatus(buildOpForStatus(tc.steps)); got != tc.want {
				t.Fatalf("DeriveStatus = %s, want %s", got, tc.want)
			}
		})
	}
}

func buildOpForStatus(steps map[string]StepStatus) *Operation {
	op := &Operation{Steps: map[string]*Step{}}
	for id, status := range steps {
		op.Order = append(op.Order, id)
		op.Steps[id] = &Step{ID: id, Status: status}
	}
	return op
}
