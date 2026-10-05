// Package workerpool implements a bounded, retrying work executor.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed indicates that a pool no longer accepts work.
var ErrClosed = errors.New("worker pool is closed")

// Job is one bounded unit of work.
type Job func(context.Context) error

// ResultStatus describes the final outcome of one submitted job.
type ResultStatus string

const (
	// ResultSucceeded marks a completed worker result.
	ResultSucceeded ResultStatus = "succeeded"

	// ResultFailed marks a worker result that ended with an error.
	ResultFailed ResultStatus = "failed"

	// ResultTimedOut marks a worker result that exceeded its deadline.
	ResultTimedOut ResultStatus = "timed_out"
	// ResultCancelled marks a worker result cancelled before completion.
	ResultCancelled ResultStatus = "cancelled"
)

// Result is emitted exactly once for every job that starts executing.
type Result struct {
	ID         string
	Status     ResultStatus
	Err        error
	Attempts   int
	Retried    bool
	StartedAt  time.Time
	FinishedAt time.Time
}

type submission struct {
	id  string
	job Job
}

// Config controls worker and retry behavior.
type Config struct {
	Workers    int
	QueueSize  int
	Retries    int
	Timeout    time.Duration
	RetryDelay time.Duration
}

// Pool executes jobs with fixed concurrency and bounded memory.
type Pool struct {
	cfg        Config
	jobs       chan submission
	slots      chan struct{}
	done       chan struct{}
	rawResults chan Result
	results    chan Result
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	resultWG   sync.WaitGroup
	once       sync.Once
	mu         sync.Mutex
	started    bool
	closed     bool
	sequence   atomic.Uint64
}

// New validates config and constructs an unstarted Pool.
func New(cfg Config) (*Pool, error) {
	if cfg.Workers < 1 || cfg.QueueSize < 1 {
		return nil, fmt.Errorf("workers and queue size must be positive")
	}
	if cfg.Retries < 0 || cfg.Timeout <= 0 || cfg.RetryDelay < 0 {
		return nil, fmt.Errorf("invalid retry or timeout configuration")
	}

	slots := make(chan struct{}, cfg.QueueSize)
	for i := 0; i < cfg.QueueSize; i++ {
		slots <- struct{}{}
	}

	resultCapacity := cfg.QueueSize + cfg.Workers

	return &Pool{
		cfg:        cfg,
		jobs:       make(chan submission, cfg.QueueSize),
		slots:      slots,
		done:       make(chan struct{}),
		rawResults: make(chan Result, resultCapacity),
		results:    make(chan Result, resultCapacity),
	}, nil
}

// Start launches the fixed worker set. Cancellation stops workers promptly.
func (p *Pool) Start(parent context.Context) {
	p.mu.Lock()
	if p.started || p.closed {
		p.mu.Unlock()
		return
	}

	p.started = true
	ctx, cancel := context.WithCancel(parent)
	p.cancel = cancel

	p.resultWG.Add(1)
	go p.aggregate()

	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	p.mu.Unlock()
}

// Submit queues a job or returns when the caller is cancelled or the pool is closed.
func (p *Pool) Submit(ctx context.Context, job Job) error {
	id := fmt.Sprintf("job-%d", p.sequence.Add(1))
	return p.SubmitNamed(ctx, id, job)
}

// SubmitNamed queues a job with an identifier carried through the result path.
func (p *Pool) SubmitNamed(ctx context.Context, id string, job Job) error {
	if job == nil {
		return fmt.Errorf("nil job")
	}
	if id == "" {
		return fmt.Errorf("job ID is required")
	}

	select {
	case <-p.slots:
	case <-ctx.Done():
		return ctx.Err()
	case <-p.done:
		return ErrClosed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		p.slots <- struct{}{}
		return ErrClosed
	}

	p.jobs <- submission{id: id, job: job}
	return nil
}

// Results exposes the bounded, typed fan-in stream.
func (p *Pool) Results() <-chan Result {
	return p.results
}

// Stop prevents submissions, cancels workers, and waits for all goroutines.
func (p *Pool) Stop() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.done)
		cancel := p.cancel
		p.mu.Unlock()

		if cancel != nil {
			cancel()
		}

		p.wg.Wait()
		close(p.rawResults)
		p.resultWG.Wait()
	})
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		select {
		case <-ctx.Done():
			return
		case submitted := <-p.jobs:
			p.slots <- struct{}{}

			if submitted.job != nil {
				result := p.run(ctx, submitted)
				p.rawResults <- result
			}
		}
	}
}

func (p *Pool) run(ctx context.Context, submitted submission) Result {
	result := Result{
		ID:        submitted.id,
		StartedAt: time.Now(),
	}

	for attempt := 0; attempt <= p.cfg.Retries; attempt++ {
		result.Attempts = attempt + 1

		jobCtx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
		err := submitted.job(jobCtx)
		jobErr := jobCtx.Err()
		cancel()

		if err == nil && jobErr == nil {
			result.Status = ResultSucceeded
			result.FinishedAt = time.Now()
			return result
		}

		if err == nil {
			err = jobErr
		}

		result.Err = err

		if ctx.Err() != nil || errors.Is(err, context.Canceled) {
			result.Status = ResultCancelled
			result.FinishedAt = time.Now()
			return result
		}

		if errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(jobErr, context.DeadlineExceeded) {
			result.Status = ResultTimedOut
		} else {
			result.Status = ResultFailed
		}

		if attempt < p.cfg.Retries {
			result.Retried = true
			timer := time.NewTimer(p.cfg.RetryDelay)

			select {
			case <-ctx.Done():
				timer.Stop()
				result.Status = ResultCancelled
				result.Err = ctx.Err()
				result.FinishedAt = time.Now()
				return result
			case <-timer.C:
			}
		}
	}

	result.FinishedAt = time.Now()
	return result
}

// aggregate is the single fan-in path from all workers to the public result stream.
func (p *Pool) aggregate() {
	defer p.resultWG.Done()
	defer close(p.results)

	for result := range p.rawResults {
		p.results <- result
	}
}
