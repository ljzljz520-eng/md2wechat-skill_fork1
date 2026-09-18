package saga

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
)

// IsAmbiguousError reports whether a side-effect call may have reached the
// backend while its outcome was not observed. Such failures must not be
// retried blindly: the step is moved to unknown and reconciled before any
// retry. Classification is intentionally limited to deterministic transport
// signals; parsed backend error codes are treated as definite failures.
func IsAmbiguousError(err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, context.Canceled),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.ECONNABORTED),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, syscall.ETIMEDOUT):
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	message := strings.ToLower(err.Error())
	for _, marker := range ambiguousMarkers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

var ambiguousMarkers = []string{
	"timeout",
	"deadline exceeded",
	"connection reset",
	"connection refused",
	"broken pipe",
	"unexpected eof",
	"tls handshake timeout",
	"client.timeout exceeded",
	"temporarily unavailable",
	"network is unreachable",
	"no such host",
	"server misbehaving",
}

// AmbiguousStepError is returned when an unknown step cannot be reconciled and
// the caller must not perform the side effect again without human judgment.
type AmbiguousStepError struct {
	OperationID string
	StepID      string
	Reason      string
}

// Error implements error.
func (e *AmbiguousStepError) Error() string {
	if e == nil {
		return "ambiguous saga step"
	}
	reason := e.Reason
	if reason == "" {
		return "saga step " + e.StepID + " has an unknown remote state; run reconcile before retry"
	}
	return "saga step " + e.StepID + " has an unknown remote state: " + reason
}
