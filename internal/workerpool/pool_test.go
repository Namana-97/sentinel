package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolBoundedAndRetry(t *testing.T) {
	pool, err := New(Config{Workers: 2, QueueSize: 4, Retries: 2, Timeout: time.Second, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	var attempts atomic.Int32
	done := make(chan struct{})
	if err := pool.Submit(ctx, func(context.Context) error {
		if attempts.Add(1) < 3 {
			return context.DeadlineExceeded
		}
		close(done)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("job did not retry")
	}
	result := <-pool.Results()
	if result.ID == "" || result.Status != ResultSucceeded || result.Attempts != 3 || !result.Retried {
		t.Fatalf("unexpected result: %#v", result)
	}
	pool.Stop()
}
func TestPoolCancellation(t *testing.T) {
	pool, _ := New(Config{Workers: 1, QueueSize: 1, Timeout: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	pool.Start(ctx)
	started := make(chan struct{})
	_ = pool.Submit(ctx, func(jobCtx context.Context) error { close(started); <-jobCtx.Done(); return jobCtx.Err() })
	<-started
	cancel()
	stopped := make(chan struct{})
	go func() { pool.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("pool leaked worker after cancellation")
	}
	result := <-pool.Results()
	if result.Status != ResultCancelled || !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("unexpected cancellation result: %#v", result)
	}
	if err := pool.Submit(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Submit() error = %v", err)
	}
}

func TestPoolSaturationAndTimeoutResult(t *testing.T) {
	pool, err := New(Config{Workers: 1, QueueSize: 1, Timeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool.Start(ctx)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := pool.SubmitNamed(ctx, "running", func(context.Context) error { close(started); <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := pool.SubmitNamed(ctx, "queued", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	blockedCtx, blockedCancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer blockedCancel()
	if err := pool.SubmitNamed(blockedCtx, "overflow", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated submit error=%v", err)
	}
	close(release)
	for i := 0; i < 2; i++ {
		<-pool.Results()
	}
	pool.Stop()

	timeoutPool, _ := New(Config{Workers: 1, QueueSize: 1, Timeout: 10 * time.Millisecond})
	timeoutPool.Start(context.Background())
	_ = timeoutPool.SubmitNamed(context.Background(), "timeout", func(jobCtx context.Context) error { <-jobCtx.Done(); return jobCtx.Err() })
	result := <-timeoutPool.Results()
	if result.ID != "timeout" || result.Status != ResultTimedOut {
		t.Fatalf("unexpected timeout result: %#v", result)
	}
	timeoutPool.Stop()
}

func TestPoolFanInConcurrentResults(t *testing.T) {
	pool, err := New(Config{Workers: 4, QueueSize: 16, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	pool.Start(context.Background())
	for i := 0; i < 16; i++ {
		id := "concurrent-" + string(rune('a'+i))
		if err := pool.SubmitNamed(context.Background(), id, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]bool)
	for i := 0; i < 16; i++ {
		result := <-pool.Results()
		if result.Status != ResultSucceeded {
			t.Fatalf("result=%#v", result)
		}
		seen[result.ID] = true
	}
	if len(seen) != 16 {
		t.Fatalf("fan-in lost results: %d", len(seen))
	}
	pool.Stop()
}
