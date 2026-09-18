package main

import (
	"os"
	"testing"
)

// TestMain keeps the deterministic test suite away from the user's real
// ~/.config/md2wechat/saga journal: saga instrumentation is disabled unless a
// test explicitly opts in with MD2WECHAT_SAGA=on.
func TestMain(m *testing.M) {
	if os.Getenv("MD2WECHAT_SAGA") == "" {
		_ = os.Setenv("MD2WECHAT_SAGA", "off")
	}
	os.Exit(m.Run())
}
