package saga

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
)

func TestIsAmbiguousErrorMatrix(t *testing.T) {
	ambiguous := []error{
		context.DeadlineExceeded,
		context.Canceled,
		io.EOF,
		io.ErrUnexpectedEOF,
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.EPIPE,
		syscall.ETIMEDOUT,
		errors.New("read tcp: i/o timeout"),
		errors.New("tls handshake timeout"),
		errors.New("connection reset by peer"),
	}
	for _, err := range ambiguous {
		if !IsAmbiguousError(err) {
			t.Errorf("IsAmbiguousError(%v) = false, want true", err)
		}
	}

	definite := []error{
		nil,
		errors.New("40007 invalid media id"),
		errors.New("40004 media type missing"),
		errors.New("draft content size out of limit"),
	}
	for _, err := range definite {
		if IsAmbiguousError(err) {
			t.Errorf("IsAmbiguousError(%v) = true, want false", err)
		}
	}
}
