package relay

import (
	"errors"
	"testing"
)

func TestRetryNil(t *testing.T) {
	if got := Retry(nil); got != nil {
		t.Errorf("Retry(nil) = %v, want nil", got)
	}
}

func TestDiscardNil(t *testing.T) {
	if got := Discard(nil); got != nil {
		t.Errorf("Discard(nil) = %v, want nil", got)
	}
}

func TestIsRetry(t *testing.T) {
	base := errors.New("timeout")
	err := Retry(base)
	if !IsRetry(err) {
		t.Error("IsRetry should return true for Retry-wrapped error")
	}
	if IsDiscard(err) {
		t.Error("IsDiscard should return false for Retry-wrapped error")
	}
	if !errors.Is(err, base) {
		t.Error("Retry error should unwrap to base error")
	}
}

func TestIsDiscard(t *testing.T) {
	base := errors.New("invalid user")
	err := Discard(base)
	if !IsDiscard(err) {
		t.Error("IsDiscard should return true for Discard-wrapped error")
	}
	if IsRetry(err) {
		t.Error("IsRetry should return false for Discard-wrapped error")
	}
	if !errors.Is(err, base) {
		t.Error("Discard error should unwrap to base error")
	}
}

func TestRetryErrorMessage(t *testing.T) {
	err := Retry(errors.New("connection refused"))
	want := "retry: connection refused"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

func TestDiscardErrorMessage(t *testing.T) {
	err := Discard(errors.New("bad payload"))
	want := "discard: bad payload"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

func TestPlainErrorIsNotRetryOrDiscard(t *testing.T) {
	err := errors.New("something")
	if IsRetry(err) {
		t.Error("plain error should not be retry")
	}
	if IsDiscard(err) {
		t.Error("plain error should not be discard")
	}
}
