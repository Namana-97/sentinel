// Package cgroup discovers pod cgroups and reads Linux memory controllers.
package cgroup

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/namanakanchan/sentinel/internal/policy"
)

// ErrUnlimited indicates that the kernel reports no finite memory ceiling.
var ErrUnlimited = errors.New("cgroup has no finite memory limit")

// Reader reads and signals pod cgroups from a mounted host cgroup filesystem.
type Reader struct {
	Root  string
	mu    sync.RWMutex
	paths map[string]string
}

// NewReader creates a reader with a concurrency-safe pod path cache.
func NewReader(root string) *Reader { return &Reader{Root: root, paths: make(map[string]string)} }

// FindPod locates a pod cgroup by Kubernetes UID for systemd or cgroupfs layouts.
func (r *Reader) FindPod(ctx context.Context, uid string) (string, error) {
	r.mu.RLock()
	cached := r.paths[uid]
	r.mu.RUnlock()
	if cached != "" && (fileExists(filepath.Join(cached, "memory.current")) || fileExists(filepath.Join(cached, "memory.usage_in_bytes"))) {
		return cached, nil
	}
	needle := strings.ReplaceAll(uid, "-", "_")
	var found string
	err := filepath.WalkDir(r.Root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, fs.ErrPermission) {
				return fs.SkipDir
			}
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		name := strings.ReplaceAll(entry.Name(), "-", "_")
		if strings.Contains(name, needle) && (fileExists(filepath.Join(path, "memory.current")) || fileExists(filepath.Join(path, "memory.usage_in_bytes"))) {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("scan cgroups: %w", err)
	}
	if found == "" {
		return "", fmt.Errorf("pod cgroup %s: %w", uid, fs.ErrNotExist)
	}
	r.mu.Lock()
	if len(r.paths) >= 4096 {
		r.paths = make(map[string]string)
	}
	r.paths[uid] = found
	r.mu.Unlock()
	return found, nil
}

// Read detects cgroup v2 or v1 and returns exact kernel-reported values.
func (r *Reader) Read(ctx context.Context, path string) (policy.Reading, error) {
	if err := ctx.Err(); err != nil {
		return policy.Reading{}, err
	}
	if fileExists(filepath.Join(path, "memory.current")) {
		return readFiles(path, "memory.current", "memory.max")
	}
	return readFiles(path, "memory.usage_in_bytes", "memory.limit_in_bytes")
}

// SignalTERM sends SIGTERM to processes in the pod cgroup and all container descendants.
func (r *Reader) SignalTERM(ctx context.Context, path string) error {
	seen := make(map[int]struct{})
	var errs []error
	walkErr := filepath.WalkDir(path, func(current string, entry fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || entry.Name() != "cgroup.procs" {
			return nil
		}
		data, err := os.ReadFile(current)
		if err != nil {
			errs = append(errs, fmt.Errorf("read %s: %w", current, err))
			return nil
		}
		for _, field := range strings.Fields(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if _, exists := seen[pid]; exists {
				continue
			}
			seen[pid] = struct{}{}
			if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
				errs = append(errs, fmt.Errorf("signal pid %d: %w", pid, err))
			}
		}
		return nil
	})
	if walkErr != nil {
		errs = append(errs, fmt.Errorf("walk cgroup processes: %w", walkErr))
	}
	return errors.Join(errs...)
}

func readFiles(path, usageFile, limitFile string) (policy.Reading, error) {
	usage, err := readUint(filepath.Join(path, usageFile))
	if err != nil {
		return policy.Reading{}, err
	}
	limitData, err := os.ReadFile(filepath.Join(path, limitFile))
	if err != nil {
		return policy.Reading{}, fmt.Errorf("read limit: %w", err)
	}
	if strings.TrimSpace(string(limitData)) == "max" {
		return policy.Reading{}, ErrUnlimited
	}
	limit, err := strconv.ParseUint(strings.TrimSpace(string(limitData)), 10, 64)
	if err != nil {
		return policy.Reading{}, fmt.Errorf("parse limit: %w", err)
	}
	if limit >= uint64(^uint64(0)>>1)-4095 {
		return policy.Reading{}, ErrUnlimited
	}
	return policy.Reading{Usage: usage, Limit: limit}, nil
}
func readUint(path string) (uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read usage: %w", err)
	}
	value, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse usage: %w", err)
	}
	return value, nil
}
func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
