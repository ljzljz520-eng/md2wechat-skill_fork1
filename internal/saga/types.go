// Package saga provides an append-only operation journal for side-effecting
// publish workflows. Every remote effect (material upload, draft creation) is
// recorded as an idempotent step with its input digest, remote identifier,
// compensation action, and verifiable postcondition, so interrupted runs can
// be resumed or reconciled without duplicate side effects.
package saga

import (
	"encoding/json"
	"time"
)

// SchemaVersion is the on-disk journal schema version.
const SchemaVersion = "1"

// OpKind identifies the orchestrated workflow kind.
type OpKind string

const (
	// OpKindPublish is the convert --upload/--draft publish pipeline.
	OpKindPublish OpKind = "publish"
)

// StepKind identifies a remote side-effect type.
type StepKind string

const (
	// StepKindUploadMaterial is a WeChat permanent image material upload.
	StepKindUploadMaterial StepKind = "upload_material"
	// StepKindCreateDraft is a WeChat draft creation.
	StepKindCreateDraft StepKind = "create_draft"
)

// StepStatus is the replayed state of one saga step.
type StepStatus string

const (
	StepStatusPending   StepStatus = "pending"
	StepStatusSucceeded StepStatus = "succeeded"
	StepStatusFailed    StepStatus = "failed"
	StepStatusUnknown   StepStatus = "unknown"
)

// OpStatus is the final tri-state outcome reported for an operation.
type OpStatus string

const (
	// OpStatusCompleted means every planned step has a verified effect.
	OpStatusCompleted OpStatus = "completed"
	// OpStatusPartial means at least one step is done and remaining steps are
	// deterministically failed or still pending.
	OpStatusPartial OpStatus = "partial"
	// OpStatusUnknown means at least one step may have succeeded remotely while
	// its response was lost; human or reconcile action is required.
	OpStatusUnknown OpStatus = "unknown"
)

// Compensation kinds recorded for manual remediation.
const (
	CompensationDeleteMaterial = "delete_material"
	CompensationDeleteDraft    = "delete_draft"
)

// Verification methods explain how a remote effect was established.
const (
	// VerifiedByWorkResult: the local call returned the remote identifier.
	VerifiedByWorkResult = "work_result"
	// VerifiedByRemoteID: the recorded identifier was confirmed present remotely.
	VerifiedByRemoteID = "remote_id"
	// VerifiedByContentFingerprint: a remote object matched the recorded digest.
	VerifiedByContentFingerprint = "content_fingerprint"
	// VerifiedByWindowUnique: exactly one remote object appeared in the attempt window.
	VerifiedByWindowUnique = "window_unique"
)

// Effect is the remote identifier(s) produced by one side effect.
type Effect struct {
	RemoteID  string `json:"remote_id,omitempty"`
	RemoteURL string `json:"remote_url,omitempty"`
}

// Compensation describes the inverse action for a verified effect. It is
// recorded for audit/manual remediation; the saga never executes it
// automatically.
type Compensation struct {
	Kind     string `json:"kind"`
	RemoteID string `json:"remote_id"`
}

// PostCondition is the verifiable descriptor recorded after an effect.
type PostCondition struct {
	Kind          string         `json:"kind"`
	RemoteID      string         `json:"remote_id"`
	RemoteURL     string         `json:"remote_url,omitempty"`
	ContentDigest string         `json:"content_digest,omitempty"`
	Extra         map[string]any `json:"extra,omitempty"`
}

// Attempt records one execution try of a step.
type Attempt struct {
	Number    int       `json:"number"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	Error     string    `json:"error,omitempty"`
	// Ambiguous is true when the request may have reached the backend while the
	// response could not be observed (timeout, connection reset, ...).
	Ambiguous bool `json:"ambiguous,omitempty"`
}

// Step is the replayed state of one idempotent saga step.
type Step struct {
	ID             string         `json:"id"`
	Kind           StepKind       `json:"kind"`
	IdempotencyKey string         `json:"idempotency_key"`
	InputDigest    string         `json:"input_digest"`
	Status         StepStatus     `json:"status"`
	Effect         *Effect        `json:"effect,omitempty"`
	Compensation   *Compensation  `json:"compensation,omitempty"`
	PostCondition  *PostCondition `json:"postcondition,omitempty"`
	Attempts       []Attempt      `json:"attempts,omitempty"`
	VerifiedMethod string         `json:"verified_method,omitempty"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// LastAttempt returns the most recent attempt or nil.
func (s *Step) LastAttempt() *Attempt {
	if s == nil || len(s.Attempts) == 0 {
		return nil
	}
	return &s.Attempts[len(s.Attempts)-1]
}

// Operation is the replayed state of one saga operation.
type Operation struct {
	ID            string           `json:"id"`
	SchemaVersion string           `json:"schema_version"`
	Kind          OpKind           `json:"kind"`
	InputDigest   string           `json:"input_digest"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
	Request       json.RawMessage  `json:"request,omitempty"`
	Steps         map[string]*Step `json:"-"`
	Order         []string         `json:"-"`
	Terminal      *OpStatus        `json:"-"`
	TerminalAt    *time.Time       `json:"-"`
}

// Succeeded reports whether the operation reached a completed terminal event.
func (o *Operation) IsTerminalCompleted() bool {
	return o != nil && o.Terminal != nil && *o.Terminal == OpStatusCompleted
}

// StepReport is one step entry in a saga report.
type StepReport struct {
	ID             string         `json:"id"`
	Kind           StepKind       `json:"kind"`
	Status         StepStatus     `json:"status"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	InputDigest    string         `json:"input_digest,omitempty"`
	Effect         *Effect        `json:"effect,omitempty"`
	PostCondition  *PostCondition `json:"postcondition,omitempty"`
	Compensation   *Compensation  `json:"compensation,omitempty"`
	Attempts       int            `json:"attempts"`
	VerifiedMethod string         `json:"verified_method,omitempty"`
	LastError      string         `json:"last_error,omitempty"`
	Ambiguous      bool           `json:"ambiguous,omitempty"`
	UpdatedAt      time.Time      `json:"updated_at,omitempty"`
}

// ManualAction is one human-handling checklist entry.
type ManualAction struct {
	StepID       string        `json:"step_id"`
	Severity     string        `json:"severity"`
	Action       string        `json:"action"`
	Detail       string        `json:"detail"`
	Compensation *Compensation `json:"compensation,omitempty"`
}

// Report is the observable saga outcome returned to callers.
type Report struct {
	OperationID   string         `json:"operation_id"`
	Kind          OpKind         `json:"kind"`
	Status        OpStatus       `json:"status"`
	InputDigest   string         `json:"input_digest"`
	JournalPath   string         `json:"journal_path"`
	Steps         []StepReport   `json:"steps"`
	ManualActions []ManualAction `json:"manual_actions"`
}

// Summary is the lightweight operation listing entry.
type Summary struct {
	OperationID string    `json:"operation_id"`
	Kind        OpKind    `json:"kind"`
	Status      OpStatus  `json:"status"`
	InputDigest string    `json:"input_digest"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	StepTotal   int       `json:"step_total"`
	StepDone    int       `json:"step_done"`
	JournalPath string    `json:"journal_path"`
}
