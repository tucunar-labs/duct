// Package reload detects changes in a folder of configuration files so
// `duct serve` can swap them in without a restart (semantic models, jobs).
// It polls and hashes the content: it needs no OS watcher, works on any
// filesystem and is immune to editors that rename or truncate files.
package reload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// DefaultInterval is how often serve looks for changes.
const DefaultInterval = 2 * time.Second

// Signature hashes the names and contents of the files of dir that match any
// of the glob patterns. A missing folder has the empty signature of no files.
func Signature(dir string, patterns ...string) (string, error) {
	var files []string
	for _, p := range patterns {
		fs, err := filepath.Glob(filepath.Join(dir, p))
		if err != nil {
			return "", err
		}
		files = append(files, fs...)
	}
	sort.Strings(files)
	h := sha256.New()
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			if os.IsNotExist(err) { // removed while we looked
				continue
			}
			return "", err
		}
		h.Write([]byte(filepath.Base(f)))
		h.Write([]byte{0})
		h.Write(data)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Poll calls changed(sig) every interval (default DefaultInterval) in which
// the signature of dir differs from the last one seen (initially last). A
// changed set is reported once: the caller logs it if it is invalid, and the
// next call is for the next edit. Poll returns when ctx ends.
func Poll(ctx context.Context, interval time.Duration, dir string, patterns []string, last string, changed func(sig string)) {
	if interval <= 0 {
		interval = DefaultInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		sig, err := Signature(dir, patterns...)
		if err != nil || sig == last {
			continue
		}
		last = sig
		changed(sig)
	}
}
