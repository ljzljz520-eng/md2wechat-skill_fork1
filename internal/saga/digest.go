package saga

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// DigestBytes returns the hex SHA-256 of raw bytes.
func DigestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DigestReader returns the hex SHA-256 of a reader stream.
func DigestReader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// DigestFile returns the hex SHA-256 of a file's bytes.
func DigestFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	return DigestReader(f)
}

// DigestStrings returns a stable digest over domain-prefixed string parts.
func DigestStrings(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s\x00", len(part), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DigestJSON returns the hex SHA-256 of the canonical JSON encoding of v.
// Go's encoding/json emits map keys in sorted order, so equal values digest
// identically regardless of map construction order.
func DigestJSON(v any) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return DigestBytes(data), nil
}

// MustDigestJSON panics on marshal failure; use only with statically-safe
// values (fixed structs without channels/functions).
func MustDigestJSON(v any) string {
	digest, err := DigestJSON(v)
	if err != nil {
		panic(err)
	}
	return digest
}
