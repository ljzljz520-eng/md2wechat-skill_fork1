package saga

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	return NewStore(t.TempDir())
}

func fixedClock() func() time.Time {
	cur := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	return func() time.Time {
		cur = cur.Add(time.Second)
		return cur
	}
}

func beginTestOp(t *testing.T, store *Store, opID, inputDigest string, clock func() time.Time) *Journal {
	t.Helper()
	j, err := store.Begin(opID, OpKindPublish, inputDigest, json.RawMessage(`{}`), clock)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	return j
}

func planEffect(t *testing.T, j *Journal, stepID string) {
	t.Helper()
	if _, err := j.PlanStep(stepID, StepKindUploadMaterial, "digest-"+stepID); err != nil {
		t.Fatalf("PlanStep: %v", err)
	}
	attempt, err := j.AttemptStarted(stepID)
	if err != nil {
		t.Fatalf("AttemptStarted: %v", err)
	}
	if err := j.StepEffect(stepID, attempt,
		Effect{RemoteID: "remote-" + stepID, RemoteURL: "https://example/" + stepID},
		Compensation{Kind: CompensationDeleteMaterial, RemoteID: "remote-" + stepID},
		PostCondition{Kind: "wechat_permanent_image", RemoteID: "remote-" + stepID},
	); err != nil {
		t.Fatalf("StepEffect: %v", err)
	}
}

func TestJournalLifecycleAndReplay(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-abc", "input-1", clock)
	planEffect(t, j, "asset:0")
	planEffect(t, j, "asset:1")
	if err := j.Terminal(OpStatusCompleted); err != nil {
		t.Fatalf("Terminal: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	op, err := ReplayFile(store.journalPath("pub-abc"))
	if err != nil {
		t.Fatalf("ReplayFile: %v", err)
	}
	if op.ID != "pub-abc" || op.InputDigest != "input-1" {
		t.Fatalf("op = %#v", op)
	}
	if len(op.Order) != 2 || op.Order[0] != "asset:0" {
		t.Fatalf("order = %#v", op.Order)
	}
	for _, id := range op.Order {
		step := op.Steps[id]
		if step.Status != StepStatusSucceeded || step.Effect == nil || step.Effect.RemoteID != "remote-"+id {
			t.Fatalf("step %s = %#v", id, step)
		}
	}
	if !op.IsTerminalCompleted() {
		t.Fatalf("terminal = %#v", op.Terminal)
	}

	found, err := store.FindByInputDigest(OpKindPublish, "input-1")
	if err != nil || found.ID != "pub-abc" {
		t.Fatalf("FindByInputDigest = %v, %v", found, err)
	}
	items, err := store.List()
	if err != nil || len(items) != 1 || items[0].OperationID != "pub-abc" || items[0].StepDone != 2 {
		t.Fatalf("List = %#v, %v", items, err)
	}
}

func TestDeterministicFailureIsRetriableAcrossResume(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-retry", "input", clock)
	if _, err := j.PlanStep("asset:0", StepKindUploadMaterial, "d"); err != nil {
		t.Fatal(err)
	}
	attempt, err := j.AttemptStarted("asset:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AttemptFailed("asset:0", attempt, errors.New("400 invalid media type"), false); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	j2, err := store.Resume("pub-retry", OpKindPublish, "input", clock)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	defer func() { _ = j2.Close() }()
	step := j2.Op().Steps["asset:0"]
	if step.Status != StepStatusFailed {
		t.Fatalf("status = %s, want failed", step.Status)
	}
	// A deterministic failure must not poison later success.
	next, err := j2.AttemptStarted("asset:0")
	if err != nil || next != 2 {
		t.Fatalf("attempt = %d, %v", next, err)
	}
	if err := j2.StepEffect("asset:0", next, Effect{RemoteID: "r"}, Compensation{Kind: CompensationDeleteMaterial, RemoteID: "r"}, PostCondition{Kind: "k", RemoteID: "r"}); err != nil {
		t.Fatal(err)
	}
	if step.Status != StepStatusSucceeded {
		t.Fatalf("status = %s", step.Status)
	}
}

func TestAmbiguousFailureMovesStepToUnknown(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-amb", "input", clock)
	if _, err := j.PlanStep("asset:0", StepKindUploadMaterial, "d"); err != nil {
		t.Fatal(err)
	}
	attempt, _ := j.AttemptStarted("asset:0")
	if err := j.AttemptFailed("asset:0", attempt, errors.New("read tcp: i/o timeout"), true); err != nil {
		t.Fatal(err)
	}
	if j.Op().Steps["asset:0"].Status != StepStatusUnknown {
		t.Fatal("ambiguous failure did not move step to unknown")
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	op, err := ReplayFile(store.journalPath("pub-amb"))
	if err != nil {
		t.Fatal(err)
	}
	if op.Steps["asset:0"].Status != StepStatusUnknown {
		t.Fatalf("replayed status = %s", op.Steps["asset:0"].Status)
	}
	last := op.Steps["asset:0"].LastAttempt()
	if last == nil || !last.Ambiguous {
		t.Fatalf("last attempt = %#v", last)
	}
}

func TestResumeRejectsDifferentInput(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-x", "input-a", clock)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resume("pub-x", OpKindPublish, "input-b", clock); !errors.Is(err, ErrInputDigestMismatch) {
		t.Fatalf("Resume err = %v, want ErrInputDigestMismatch", err)
	}
}

func TestCompletedOperationDoesNotRegress(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-done", "input", clock)
	planEffect(t, j, "asset:0")
	if err := j.Terminal(OpStatusCompleted); err != nil {
		t.Fatal(err)
	}
	if err := j.Terminal(OpStatusPartial); !errors.Is(err, ErrOperationCompleted) {
		t.Fatalf("Terminal regression err = %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// Resume of a completed operation is read-only (no execution lock held).
	j2, err := store.Resume("pub-done", OpKindPublish, "input", clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j2.Close() }()
	if j2.lock != nil {
		t.Fatal("completed operation was reopened with an execution lock")
	}
}

func TestLockConflict(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-lock", "input", clock)
	defer func() { _ = j.Close() }()

	second, err := store.openLocked(store.journalPath("pub-lock"), clock)
	if !errors.Is(err, ErrOperationInProgress) {
		if second != nil {
			_ = second.Close()
		}
		t.Fatalf("second open err = %v, want ErrOperationInProgress", err)
	}
}

func TestTornTailCrashMatrix(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()

	tests := []struct {
		name      string
		eventType string
		want      StepStatus
	}{
		{"attempt started may have dispatched", EventAttemptStart, StepStatusUnknown},
		{"attempt failed may have dispatched", EventAttemptFailed, StepStatusUnknown},
		{"effect write torn", EventStepEffect, StepStatusUnknown},
		{"step reset torn", EventStepReset, StepStatusUnknown},
		{"step verified torn", EventStepVerified, StepStatusUnknown},
		{"step planned is local only", EventStepPlanned, StepStatusSucceeded},
		{"terminal is local only", EventOpTerminal, StepStatusSucceeded},
		{"reuse marker is local only", EventStepReused, StepStatusSucceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opID := "pub-torn-" + strings.ReplaceAll(tc.eventType, "_", "-")
			j := beginTestOp(t, store, opID, "input", clock)
			planEffect(t, j, "asset:0")
			path := store.journalPath(opID)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			torn := fmt.Sprintf(`{"type":%q,"ts":"2025-01-02T03:05:00Z","step_id":"asset:0"`, tc.eventType)
			if err := os.WriteFile(path, append(mustReadFile(t, path), []byte(torn)...), 0o600); err != nil {
				t.Fatal(err)
			}
			op, err := ReplayFile(path)
			if err != nil {
				t.Fatalf("ReplayFile: %v", err)
			}
			if got := op.Steps["asset:0"].Status; got != tc.want {
				t.Fatalf("status = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestTornTailUnreadableRecordRejected(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-garbage", "input", clock)
	path := store.journalPath("pub-garbage")
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(mustReadFile(t, path), []byte(`{"type":"un`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayFile(path); !errors.Is(err, ErrJournalTruncated) {
		t.Fatalf("err = %v, want ErrJournalTruncated", err)
	}
}

func TestCorruptMiddleLineRejected(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-corrupt", "input", clock)
	path := store.journalPath("pub-corrupt")
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// A bad line followed by a valid line is middle corruption, not a torn tail.
	payload := append(mustReadFile(t, path), []byte("{not json\n")...)
	payload = append(payload, []byte(`{"type":"op_terminal","ts":"2025-01-02T03:06:00Z","status":"partial"}`+"\n")...)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReplayFile(path); err == nil || !strings.Contains(err.Error(), "corrupt saga journal") {
		t.Fatalf("err = %v, want corrupt journal error", err)
	}
}

func TestBeginRejectsInvalidIDs(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	for _, id := range []string{"", "a/b", `a\b`} {
		if _, err := store.Begin(id, OpKindPublish, "d", nil, clock); err == nil {
			t.Fatalf("Begin(%q) succeeded", id)
		}
	}
}

func TestBeginDuplicateRejected(t *testing.T) {
	store := newTestStore(t)
	clock := fixedClock()
	j := beginTestOp(t, store, "pub-dup", "input", clock)
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin("pub-dup", OpKindPublish, "input", nil, clock); err == nil {
		t.Fatal("Begin duplicate succeeded")
	}
}

func TestOpenMissingOperation(t *testing.T) {
	store := newTestStore(t)
	if _, err := store.Open("missing"); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("Open err = %v", err)
	}
	if _, err := store.FindLatest(); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("FindLatest err = %v", err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
