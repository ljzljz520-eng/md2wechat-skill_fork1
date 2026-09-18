//go:build windows

package saga

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

const (
	lockfileExclusiveLock = 0x00000002
	lockSegment           = 0xFFFFFFFF
)

func tryLock(f *os.File) error {
	var overlapped windows.Overlapped
	err := windows.LockFileEx(windows.Handle(f.Fd()), lockfileExclusiveLock, 0, lockSegment, lockSegment, &overlapped)
	if err != nil {
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return ErrOperationInProgress
		}
		return err
	}
	return nil
}

func unlock(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, lockSegment, lockSegment, &overlapped)
}
