package saga

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/geekjourneyx/md2wechat-skill/internal/atomicfile"
)

const (
	journalFileName = "journal.jsonl"
	indexFileName   = "index.json"
	opsDirName      = "operations"
	lockFileName    = "lock"
)

// EventType enumerates the append-only journal event types.
const (
	EventOpStarted       = "op_started"
	EventOpTerminal      = "op_terminal"
	EventStepPlanned     = "step_planned"
	EventAttemptStart    = "attempt_started"
	EventAttemptFailed   = "attempt_failed"
	EventStepEffect      = "step_effect"
	EventStepVerified    = "step_verified"
	EventStepReset       = "step_reset"
	EventStepReused      = "step_reused"
	EventReconcileResult = "reconcile_result"
)

var (
	// ErrOperationInProgress is returned when another process holds the op lock.
	ErrOperationInProgress = errors.New("saga operation is locked by another process")
	// ErrOperationNotFound is returned when no journal matches the lookup.
	ErrOperationNotFound = errors.New("saga operation not found")
	// ErrInputDigestMismatch is returned when reopening an op with another input.
	ErrInputDigestMismatch = errors.New("saga operation input digest mismatch")
	// ErrJournalTruncated is returned when an unrecoverable torn tail is found.
	ErrJournalTruncated = errors.New("saga journal has an unrecoverable truncated tail")
	// ErrOperationCompleted is returned when a completed terminal op is changed.
	ErrOperationCompleted = errors.New("saga operation already completed")
)

var (
	tornTypePattern   = regexp.MustCompile(`"type":"([a-z_]+)"`)
	tornStepIDPattern = regexp.MustCompile(`"step_id":"([^"]+)"`)
)

// journalEvent is one JSONL record. Fields are shared across event types and
// omitted when empty, which keeps the on-disk format trivial to audit.
type journalEvent struct {
	Type            string          `json:"type"`
	TS              time.Time       `json:"ts"`
	OpID            string          `json:"op_id,omitempty"`
	Kind            OpKind          `json:"kind,omitempty"`
	SchemaVersion   string          `json:"schema_version,omitempty"`
	InputDigest     string          `json:"input_digest,omitempty"`
	Request         json.RawMessage `json:"request,omitempty"`
	Status          OpStatus        `json:"status,omitempty"`
	StepID          string          `json:"step_id,omitempty"`
	StepKind        StepKind        `json:"step_kind,omitempty"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	StepInputDigest string          `json:"step_input_digest,omitempty"`
	Attempt         int             `json:"attempt,omitempty"`
	Error           string          `json:"error,omitempty"`
	Ambiguous       bool            `json:"ambiguous,omitempty"`
	Effect          *Effect         `json:"effect,omitempty"`
	Compensation    *Compensation   `json:"compensation,omitempty"`
	PostCondition   *PostCondition  `json:"postcondition,omitempty"`
	Method          string          `json:"method,omitempty"`
	Outcome         string          `json:"outcome,omitempty"`
	Reason          string          `json:"reason,omitempty"`
	Candidates      []Candidate     `json:"candidates,omitempty"`
}

// Index maps "kind:input_digest" to the most recent operation for that input.
type Index struct {
	SchemaVersion string       `json:"schema_version"`
	Entries       []IndexEntry `json:"entries"`
}

// IndexEntry is one input-digest → operation pointer.
type IndexEntry struct {
	Key         string    `json:"key"`
	Kind        OpKind    `json:"kind"`
	InputDigest string    `json:"input_digest"`
	OperationID string    `json:"operation_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Journal is an append-only handle to one saga operation.
type Journal struct {
	baseDir string
	path    string
	file    *os.File
	lock    *os.File
	clock   func() time.Time
	op      *Operation
}

// Op returns the replayed operation state.
func (j *Journal) Op() *Operation { return j.op }

// Path returns the journal file path.
func (j *Journal) Path() string { return j.path }

// Store resolves on-disk saga locations.
type Store struct {
	baseDir string
}

// NewStore roots a saga store at baseDir (created lazily).
func NewStore(baseDir string) *Store {
	return &Store{baseDir: baseDir}
}

// BaseDir returns the store root.
func (s *Store) BaseDir() string { return s.baseDir }

func (s *Store) opDir(opID string) string {
	return filepath.Join(s.baseDir, opsDirName, opID)
}

func (s *Store) journalPath(opID string) string {
	return filepath.Join(s.opDir(opID), journalFileName)
}

// List replays every operation journal and returns summaries newest-first.
func (s *Store) List() ([]Summary, error) {
	root := filepath.Join(s.baseDir, opsDirName)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]Summary, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		path := filepath.Join(root, entry.Name(), journalFileName)
		if _, err := os.Stat(path); err != nil {
			continue
		}
		op, err := ReplayFile(path)
		if err != nil {
			continue
		}
		status := DeriveStatus(op)
		done := 0
		for _, id := range op.Order {
			if op.Steps[id].Status == StepStatusSucceeded {
				done++
			}
		}
		out = append(out, Summary{
			OperationID: op.ID,
			Kind:        op.Kind,
			Status:      status,
			InputDigest: op.InputDigest,
			CreatedAt:   op.CreatedAt,
			UpdatedAt:   op.UpdatedAt,
			StepTotal:   len(op.Order),
			StepDone:    done,
			JournalPath: path,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt.After(out[j].UpdatedAt) })
	return out, nil
}

// Open reads an existing operation journal without locking it for execution.
func (s *Store) Open(opID string) (*Operation, error) {
	path := s.journalPath(opID)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrOperationNotFound, opID)
		}
		return nil, err
	}
	return ReplayFile(path)
}

// FindLatest returns the most recently updated operation matching an optional
// status filter. Empty statuses returns the most recent operation.
func (s *Store) FindLatest(statuses ...OpStatus) (*Operation, error) {
	list, err := s.List()
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, ErrOperationNotFound
	}
	if len(statuses) == 0 {
		return s.Open(list[0].OperationID)
	}
	want := map[OpStatus]bool{}
	for _, status := range statuses {
		want[status] = true
	}
	for _, item := range list {
		if want[item.Status] {
			return s.Open(item.OperationID)
		}
	}
	return nil, ErrOperationNotFound
}

// FindByInputDigest resolves an operation through the input-digest index.
func (s *Store) FindByInputDigest(kind OpKind, inputDigest string) (*Operation, error) {
	idx, err := s.readIndex()
	if err != nil {
		return nil, err
	}
	key := indexKey(kind, inputDigest)
	for i := range idx.Entries {
		if idx.Entries[i].Key == key {
			return s.Open(idx.Entries[i].OperationID)
		}
	}
	return nil, ErrOperationNotFound
}

// Begin creates a new operation journal and locks it exclusively.
func (s *Store) Begin(opID string, kind OpKind, inputDigest string, request json.RawMessage, clock func() time.Time) (*Journal, error) {
	if strings.TrimSpace(opID) == "" {
		return nil, fmt.Errorf("operation id is required")
	}
	if strings.ContainsAny(opID, `/\`) {
		return nil, fmt.Errorf("invalid operation id: %s", opID)
	}
	if clock == nil {
		clock = time.Now
	}
	dir := s.opDir(opID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create saga operation dir: %w", err)
	}
	path := filepath.Join(dir, journalFileName)
	if _, err := os.Stat(path); err == nil {
		return nil, fmt.Errorf("saga operation already exists: %s", opID)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	j, err := s.openLocked(path, clock)
	if err != nil {
		return nil, err
	}
	now := clock()
	j.op = &Operation{
		ID:            opID,
		SchemaVersion: SchemaVersion,
		Kind:          kind,
		InputDigest:   inputDigest,
		CreatedAt:     now,
		UpdatedAt:     now,
		Request:       append(json.RawMessage(nil), request...),
		Steps:         map[string]*Step{},
	}
	if err := j.appendEvent(journalEvent{
		Type:          EventOpStarted,
		TS:            now,
		OpID:          opID,
		Kind:          kind,
		SchemaVersion: SchemaVersion,
		InputDigest:   inputDigest,
		Request:       request,
	}); err != nil {
		_ = j.Close()
		return nil, err
	}
	if err := s.upsertIndex(IndexEntry{
		Key:         indexKey(kind, inputDigest),
		Kind:        kind,
		InputDigest: inputDigest,
		OperationID: opID,
		UpdatedAt:   now,
	}); err != nil {
		// Index loss only degrades automatic resume discovery; journal is intact.
		_ = err
	}
	return j, nil
}

// Resume opens an existing operation journal for execution with an exclusive
// lock and verifies the operation identity.
func (s *Store) Resume(opID string, kind OpKind, inputDigest string, clock func() time.Time) (*Journal, error) {
	path := s.journalPath(opID)
	if clock == nil {
		clock = time.Now
	}
	op, err := ReplayFile(path)
	if err != nil {
		return nil, err
	}
	if kind != "" && op.Kind != kind {
		return nil, fmt.Errorf("saga operation kind mismatch: have %s, want %s", op.Kind, kind)
	}
	if inputDigest != "" && op.InputDigest != inputDigest {
		return nil, fmt.Errorf("%w: operation %s was recorded for a different input", ErrInputDigestMismatch, opID)
	}
	j, err := s.openLocked(path, clock)
	if err != nil {
		return nil, err
	}
	j.op = op
	if op.IsTerminalCompleted() {
		// Completed operations are reused read-only; callers must not execute steps.
		_ = j.unlock()
		j.lock = nil
	}
	return j, nil
}

func (s *Store) openLocked(path string, clock func() time.Time) (*Journal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	lockFile, err := os.OpenFile(filepath.Join(filepath.Dir(path), lockFileName), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := tryLock(lockFile); err != nil {
		_ = lockFile.Close()
		_ = f.Close()
		return nil, err
	}
	return &Journal{
		baseDir: s.baseDir,
		path:    path,
		file:    f,
		lock:    lockFile,
		clock:   clock,
	}, nil
}

// Close releases the lock and the journal handle.
func (j *Journal) Close() error {
	var err error
	if j.file != nil {
		err = j.file.Close()
		j.file = nil
	}
	if err2 := j.unlock(); err == nil && err2 != nil {
		err = err2
	}
	return err
}

func (j *Journal) unlock() error {
	if j.lock == nil {
		return nil
	}
	_ = unlock(j.lock)
	err := j.lock.Close()
	j.lock = nil
	return err
}

// ReopenForRead replays this journal without holding an execution lock.
func (s *Store) ReopenForRead(opID string) (*Operation, string, error) {
	op, err := s.Open(opID)
	if err != nil {
		return nil, "", err
	}
	return op, s.journalPath(opID), nil
}

func (j *Journal) appendEvent(event journalEvent) error {
	if event.TS.IsZero() {
		event.TS = j.clock()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := j.file.Write(data); err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	j.op.UpdatedAt = event.TS
	return nil
}

// PlanStep records a pending step the first time it is seen.
func (j *Journal) PlanStep(id string, kind StepKind, inputDigest string) (*Step, error) {
	if step := j.op.Steps[id]; step != nil {
		return step, nil
	}
	now := j.clock()
	step := &Step{
		ID:             id,
		Kind:           kind,
		IdempotencyKey: fmt.Sprintf("%s:%s@%s", j.op.ID, id, shortDigest(inputDigest)),
		InputDigest:    inputDigest,
		Status:         StepStatusPending,
		UpdatedAt:      now,
	}
	if err := j.appendEvent(journalEvent{
		TS:              now,
		Type:            EventStepPlanned,
		StepID:          id,
		StepKind:        kind,
		IdempotencyKey:  step.IdempotencyKey,
		StepInputDigest: inputDigest,
	}); err != nil {
		return nil, err
	}
	j.op.Steps[id] = step
	j.op.Order = append(j.op.Order, id)
	return step, nil
}

// AttemptStarted records a new attempt and returns its 1-based number.
func (j *Journal) AttemptStarted(id string) (int, error) {
	step := j.op.Steps[id]
	if step == nil {
		return 0, fmt.Errorf("unknown saga step: %s", id)
	}
	number := len(step.Attempts) + 1
	now := j.clock()
	if err := j.appendEvent(journalEvent{
		TS:      now,
		Type:    EventAttemptStart,
		StepID:  id,
		Attempt: number,
	}); err != nil {
		return 0, err
	}
	step.Attempts = append(step.Attempts, Attempt{Number: number, StartedAt: now})
	step.UpdatedAt = now
	return number, nil
}

// AttemptFailed records a failed attempt. Ambiguous failures put the step in
// the unknown state because the request may have succeeded remotely.
func (j *Journal) AttemptFailed(id string, attempt int, cause error, ambiguous bool) error {
	step := j.op.Steps[id]
	if step == nil {
		return fmt.Errorf("unknown saga step: %s", id)
	}
	now := j.clock()
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	if err := j.appendEvent(journalEvent{
		TS:        now,
		Type:      EventAttemptFailed,
		StepID:    id,
		Attempt:   attempt,
		Error:     message,
		Ambiguous: ambiguous,
	}); err != nil {
		return err
	}
	if len(step.Attempts) >= attempt && attempt > 0 {
		record := &step.Attempts[attempt-1]
		record.EndedAt = now
		record.Error = message
		record.Ambiguous = ambiguous
	}
	if ambiguous {
		step.Status = StepStatusUnknown
	} else if step.Status != StepStatusUnknown {
		step.Status = StepStatusFailed
	}
	step.UpdatedAt = now
	return nil
}

// StepEffect records a verified effect returned by the local work call.
func (j *Journal) StepEffect(id string, attempt int, effect Effect, compensation Compensation, post PostCondition) error {
	step := j.op.Steps[id]
	if step == nil {
		return fmt.Errorf("unknown saga step: %s", id)
	}
	now := j.clock()
	effectCopy := effect
	compCopy := compensation
	postCopy := post
	if err := j.appendEvent(journalEvent{
		TS:            now,
		Type:          EventStepEffect,
		StepID:        id,
		Attempt:       attempt,
		Effect:        &effectCopy,
		Compensation:  &compCopy,
		PostCondition: &postCopy,
		Method:        VerifiedByWorkResult,
	}); err != nil {
		return err
	}
	if len(step.Attempts) >= attempt && attempt > 0 {
		step.Attempts[attempt-1].EndedAt = now
	}
	step.Status = StepStatusSucceeded
	step.Effect = &effectCopy
	step.Compensation = &compCopy
	step.PostCondition = &postCopy
	step.VerifiedMethod = VerifiedByWorkResult
	step.UpdatedAt = now
	return nil
}

// StepVerified adopts a remotely established effect after reconciliation.
func (j *Journal) StepVerified(id, method string, effect Effect, compensation Compensation, post PostCondition) error {
	step := j.op.Steps[id]
	if step == nil {
		return fmt.Errorf("unknown saga step: %s", id)
	}
	now := j.clock()
	effectCopy := effect
	compCopy := compensation
	postCopy := post
	if err := j.appendEvent(journalEvent{
		TS:            now,
		Type:          EventStepVerified,
		StepID:        id,
		Effect:        &effectCopy,
		Compensation:  &compCopy,
		PostCondition: &postCopy,
		Method:        method,
	}); err != nil {
		return err
	}
	step.Status = StepStatusSucceeded
	step.Effect = &effectCopy
	step.Compensation = &compCopy
	step.PostCondition = &postCopy
	step.VerifiedMethod = method
	step.UpdatedAt = now
	return nil
}

// StepReset returns an unknown/failed step to pending after remote absence was
// verified, allowing a safe retry.
func (j *Journal) StepReset(id, method string) error {
	step := j.op.Steps[id]
	if step == nil {
		return fmt.Errorf("unknown saga step: %s", id)
	}
	now := j.clock()
	if err := j.appendEvent(journalEvent{
		TS:     now,
		Type:   EventStepReset,
		StepID: id,
		Method: method,
	}); err != nil {
		return err
	}
	step.Status = StepStatusPending
	step.Effect = nil
	step.Compensation = nil
	step.PostCondition = nil
	step.VerifiedMethod = ""
	step.UpdatedAt = now
	return nil
}

// MarkReused records that a succeeded step was served from the journal.
func (j *Journal) MarkReused(id string) error {
	step := j.op.Steps[id]
	if step == nil {
		return fmt.Errorf("unknown saga step: %s", id)
	}
	return j.appendEvent(journalEvent{
		TS:     j.clock(),
		Type:   EventStepReused,
		StepID: id,
	})
}

// ReconcileResult records a backend query verdict for auditability without
// changing step state; the caller applies the corresponding transition.
func (j *Journal) ReconcileResult(stepID, outcome, method, reason string, candidates []Candidate) error {
	if j.op.Steps[stepID] == nil {
		return fmt.Errorf("unknown saga step: %s", stepID)
	}
	now := j.clock()
	if err := j.appendEvent(journalEvent{
		TS:         now,
		Type:       EventReconcileResult,
		StepID:     stepID,
		Outcome:    outcome,
		Method:     method,
		Reason:     reason,
		Candidates: append([]Candidate(nil), candidates...),
	}); err != nil {
		return err
	}
	j.op.UpdatedAt = now
	return nil
}

// Terminal records the final operation status. Later terminal events replace
// earlier ones during replay (an interrupted retry can terminate again).
func (j *Journal) Terminal(status OpStatus) error {
	if j.op.Terminal != nil && *j.op.Terminal == OpStatusCompleted && status != OpStatusCompleted {
		// A completed operation must never regress through later journal writes.
		return fmt.Errorf("%w: %s", ErrOperationCompleted, j.op.ID)
	}
	now := j.clock()
	if err := j.appendEvent(journalEvent{
		TS:     now,
		Type:   EventOpTerminal,
		Status: status,
	}); err != nil {
		return err
	}
	statusCopy := status
	j.op.Terminal = &statusCopy
	j.op.TerminalAt = &now
	return nil
}

// ReplayFile reconstructs operation state from a JSONL journal.
func ReplayFile(path string) (*Operation, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return replay(bytes.NewReader(data))
}

func replay(r io.Reader) (*Operation, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var lines [][]byte
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		lines = append(lines, append([]byte(nil), line...))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read saga journal: %w", err)
	}

	var op *Operation
	var tornRaw []byte
	for i, line := range lines {
		var event journalEvent
		if err := json.Unmarshal(line, &event); err != nil {
			if i == len(lines)-1 {
				// A crash between write start and completion can tear the tail.
				tornRaw = line
				break
			}
			return nil, fmt.Errorf("corrupt saga journal at line %d: %w", i+1, err)
		}
		switch event.Type {
		case EventOpStarted:
			if op != nil {
				return nil, fmt.Errorf("corrupt saga journal: duplicate %s", EventOpStarted)
			}
			op = &Operation{
				ID:            event.OpID,
				SchemaVersion: event.SchemaVersion,
				Kind:          event.Kind,
				InputDigest:   event.InputDigest,
				CreatedAt:     event.TS,
				UpdatedAt:     event.TS,
				Request:       append(json.RawMessage(nil), event.Request...),
				Steps:         map[string]*Step{},
			}
		case EventOpTerminal:
			if op == nil {
				return nil, fmt.Errorf("corrupt saga journal at line %d: terminal before start", i+1)
			}
			status := event.Status
			op.Terminal = &status
			op.TerminalAt = &event.TS
		case EventStepPlanned:
			if err := requireOp(op, i); err != nil {
				return nil, err
			}
			if _, exists := op.Steps[event.StepID]; exists {
				return nil, fmt.Errorf("corrupt saga journal at line %d: duplicate step %s", i+1, event.StepID)
			}
			op.Steps[event.StepID] = &Step{
				ID:             event.StepID,
				Kind:           event.StepKind,
				IdempotencyKey: event.IdempotencyKey,
				InputDigest:    event.StepInputDigest,
				Status:         StepStatusPending,
				UpdatedAt:      event.TS,
			}
			op.Order = append(op.Order, event.StepID)
		case EventAttemptStart:
			step, err := replayStep(op, event, i)
			if err != nil {
				return nil, err
			}
			step.Attempts = append(step.Attempts, Attempt{Number: event.Attempt, StartedAt: event.TS})
		case EventAttemptFailed:
			step, err := replayStep(op, event, i)
			if err != nil {
				return nil, err
			}
			if event.Attempt > 0 && event.Attempt <= len(step.Attempts) {
				record := &step.Attempts[event.Attempt-1]
				record.EndedAt = event.TS
				record.Error = event.Error
				record.Ambiguous = event.Ambiguous
			}
			if event.Ambiguous {
				step.Status = StepStatusUnknown
			} else if step.Status != StepStatusUnknown {
				step.Status = StepStatusFailed
			}
		case EventStepEffect:
			step, err := replayStep(op, event, i)
			if err != nil {
				return nil, err
			}
			step.Status = StepStatusSucceeded
			step.Effect = cloneRef(event.Effect)
			step.Compensation = cloneRef(event.Compensation)
			step.PostCondition = cloneRef(event.PostCondition)
			step.VerifiedMethod = event.Method
			if event.Attempt > 0 && event.Attempt <= len(step.Attempts) {
				step.Attempts[event.Attempt-1].EndedAt = event.TS
			}
		case EventStepVerified:
			step, err := replayStep(op, event, i)
			if err != nil {
				return nil, err
			}
			step.Status = StepStatusSucceeded
			step.Effect = cloneRef(event.Effect)
			step.Compensation = cloneRef(event.Compensation)
			step.PostCondition = cloneRef(event.PostCondition)
			if event.Method != "" {
				step.VerifiedMethod = event.Method
			}
		case EventStepReset:
			step, err := replayStep(op, event, i)
			if err != nil {
				return nil, err
			}
			step.Status = StepStatusPending
			step.Effect = nil
			step.Compensation = nil
			step.PostCondition = nil
			step.VerifiedMethod = ""
		case EventStepReused, EventReconcileResult:
			// Observability markers only; state transitions are recorded elsewhere.
			if _, err := replayStep(op, event, i); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("corrupt saga journal at line %d: unknown event %q", i+1, event.Type)
		}
		if op != nil && event.TS.After(op.UpdatedAt) {
			op.UpdatedAt = event.TS
		}
	}
	if op == nil {
		if tornRaw != nil {
			return nil, fmt.Errorf("%w: torn %s record", ErrJournalTruncated, EventOpStarted)
		}
		return nil, fmt.Errorf("corrupt saga journal: missing %s", EventOpStarted)
	}
	if tornRaw != nil {
		if err := applyTornTail(op, tornRaw); err != nil {
			return nil, err
		}
	}
	return op, nil
}

// applyTornTail makes the conservative state transition for a partially-written
// final record: anything that may have dispatched a remote effect forces the
// affected step into unknown, so resume reconciles instead of re-executing.
func applyTornTail(op *Operation, raw []byte) error {
	typeMatch := tornTypePattern.FindSubmatch(raw)
	if typeMatch == nil {
		return fmt.Errorf("%w: unreadable event record", ErrJournalTruncated)
	}
	eventType := string(typeMatch[1])
	switch eventType {
	case EventOpTerminal, EventStepReused, EventStepPlanned:
		// Pure local bookkeeping that cannot have dispatched remote work.
		return nil
	case EventAttemptStart, EventAttemptFailed, EventStepEffect, EventStepVerified, EventStepReset:
		idMatch := tornStepIDPattern.FindSubmatch(raw)
		if idMatch == nil {
			return fmt.Errorf("%w: %s record without step id", ErrJournalTruncated, eventType)
		}
		step, ok := op.Steps[string(idMatch[1])]
		if !ok {
			return fmt.Errorf("%w: %s record for unknown step", ErrJournalTruncated, eventType)
		}
		step.Status = StepStatusUnknown
		return nil
	default:
		return fmt.Errorf("%w: unknown torn event %q", ErrJournalTruncated, eventType)
	}
}

func requireOp(op *Operation, line int) error {
	if op == nil {
		return fmt.Errorf("corrupt saga journal at line %d: event before %s", line+1, EventOpStarted)
	}
	return nil
}

func replayStep(op *Operation, event journalEvent, line int) (*Step, error) {
	if err := requireOp(op, line); err != nil {
		return nil, err
	}
	step := op.Steps[event.StepID]
	if step == nil {
		return nil, fmt.Errorf("corrupt saga journal at line %d: event for unknown step %s", line+1, event.StepID)
	}
	return step, nil
}

func cloneRef[T any](v *T) *T {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}

func shortDigest(digest string) string {
	if len(digest) <= 12 {
		return digest
	}
	return digest[:12]
}

func indexKey(kind OpKind, digest string) string {
	return string(kind) + ":" + digest
}

func (s *Store) readIndex() (*Index, error) {
	data, err := os.ReadFile(filepath.Join(s.baseDir, indexFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return &Index{SchemaVersion: SchemaVersion}, nil
		}
		return nil, err
	}
	idx := &Index{}
	if err := json.Unmarshal(data, idx); err != nil {
		return nil, fmt.Errorf("parse saga index: %w", err)
	}
	return idx, nil
}

func (s *Store) upsertIndex(entry IndexEntry) error {
	if err := os.MkdirAll(s.baseDir, 0o700); err != nil {
		return err
	}
	idx, err := s.readIndex()
	if err != nil {
		return err
	}
	replaced := false
	for i := range idx.Entries {
		if idx.Entries[i].Key == entry.Key {
			idx.Entries[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		idx.Entries = append(idx.Entries, entry)
	}
	idx.SchemaVersion = SchemaVersion
	data, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	_, err = atomicfile.Write(filepath.Join(s.baseDir, indexFileName), data)
	return err
}
