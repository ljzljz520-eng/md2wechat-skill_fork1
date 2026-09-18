//go:build !windows

package saga

import (
	"errors"
	"os"
	"syscall"
)

func tryLock(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return ErrOperationInProgress
		}
		return err
	}
	return nil
}

func unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
