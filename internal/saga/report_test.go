package saga

import (
	"strings"
	"testing"
)

func TestBuildReportManualChecklist(t *testing.T) {
	op := &Operation{ID: "pub-1", Kind: OpKindPublish, Steps: map[string]*Step{}}
	add := func(id string, kind StepKind, status StepStatus, lastErr string, amb bool, comp *Compensation) {
		step := &Step{ID: id, Kind: kind, Status: status, Compensation: comp}
		if lastErr != "" {
			step.Attempts = []Attempt{{Error: lastErr, Ambiguous: amb}}
		}
		op.Order = append(op.Order, id)
		op.Steps[id] = step
	}
	add("asset:0", StepKindUploadMaterial, StepStatusSucceeded, "", false, nil)
	add("asset:1", StepKindUploadMaterial, StepStatusUnknown, "i/o timeout", true,
		&Compensation{Kind: CompensationDeleteMaterial, RemoteID: "maybe-id"})
	add("asset:2", StepKindUploadMaterial, StepStatusFailed, "400 invalid", false, nil)
	add("draft", StepKindCreateDraft, StepStatusPending, "", false, nil)

	report := BuildReport(op, "/tmp/journal.jsonl")
	if report.Status != OpStatusUnknown {
		t.Fatalf("status = %s", report.Status)
	}
	if len(report.Steps) != 4 {
		t.Fatalf("steps = %d", len(report.Steps))
	}
	actions := map[string]ManualAction{}
	for _, item := range report.ManualActions {
		actions[item.StepID] = item
	}
	unknown, ok := actions["asset:1"]
	if !ok || unknown.Action != "reconcile_or_manual_review" || unknown.Severity != SeverityManualReview {
		t.Fatalf("unknown action = %#v", actions["asset:1"])
	}
	if unknown.Compensation == nil || unknown.Compensation.RemoteID != "maybe-id" {
		t.Fatalf("unknown compensation = %#v", unknown.Compensation)
	}
	if !strings.Contains(unknown.Detail, "saga reconcile pub-1") {
		t.Fatalf("detail missing reconcile command: %s", unknown.Detail)
	}
	if failed := actions["asset:2"]; failed.Action != "fix_and_resume" {
		t.Fatalf("failed action = %#v", failed)
	}
	if pending := actions["draft"]; pending.Action != "resume" {
		t.Fatalf("pending action = %#v", pending)
	}

	text := FormatReport(report)
	if !strings.Contains(text, "pub-1: unknown") || !strings.Contains(text, "maybe-id") {
		t.Fatalf("formatted report missing key facts:\n%s", text)
	}
}
