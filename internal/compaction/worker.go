package compaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"stravin.dev/core/internal/tenant"
)

const (
	defaultPollInterval = 2 * time.Second
	defaultGCInterval   = 5 * time.Minute
)

// RepoTarget describes a repository tracked by the daemon scheduling loop for
// compaction and delayed GC.
type RepoTarget struct {
	Repo           tenant.RepoKey
	ManifestShards int
	RefShards      int
}

// TargetDiscoverer supplies active repositories/shards to enqueue into the
// fair-share compaction queue and sweep in the delayed GC loop.
type TargetDiscoverer interface {
	DiscoverTargets(ctx context.Context) ([]ShardTarget, []RepoTarget, error)
}

// WorkerConfig configures the stravin-pack background scheduling loops.
type WorkerConfig struct {
	Engine       *Engine
	Sweeper      *GCSweeper
	Queue        *FairQueue
	Discoverer   TargetDiscoverer
	PollInterval time.Duration
	GCInterval   time.Duration
	Logger       *slog.Logger
}

// Worker runs the fair-share compaction polling loop and delayed GC sweeper
// loop cleanly separated from core compaction logic.
type Worker struct {
	engine       *Engine
	sweeper      *GCSweeper
	queue        *FairQueue
	discoverer   TargetDiscoverer
	pollInterval time.Duration
	gcInterval   time.Duration
	logger       *slog.Logger
}

// NewWorker constructs a background compaction and GC Worker.
func NewWorker(cfg WorkerConfig) (*Worker, error) {
	if cfg.Engine == nil {
		return nil, fmt.Errorf("%w: nil compaction engine", ErrInvalidConfig)
	}
	if cfg.Queue == nil {
		cfg.Queue = NewFairQueue()
	}
	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	gcInterval := cfg.GCInterval
	if gcInterval <= 0 {
		gcInterval = defaultGCInterval
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		engine:       cfg.Engine,
		sweeper:      cfg.Sweeper,
		queue:        cfg.Queue,
		discoverer:   cfg.Discoverer,
		pollInterval: pollInterval,
		gcInterval:   gcInterval,
		logger:       logger,
	}, nil
}

// Queue returns the worker's FairQueue so callers can enqueue shard targets directly.
func (w *Worker) Queue() *FairQueue {
	return w.queue
}

// Run starts the compaction polling loop and delayed GC sweeper loop, blocking
// until ctx is cancelled and all in-flight work finishes cleanly.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.compactionLoop(ctx)
	}()

	if w.sweeper != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.gcLoop(ctx)
		}()
	}

	wg.Wait()
	return nil
}

// StepCompaction drains a single item from the FairQueue (after optional
// discovery when empty) and executes it via Engine.CompactShard.
func (w *Worker) StepCompaction(ctx context.Context) (JobResult, bool, error) {
	if w.queue.Len() == 0 && w.discoverer != nil {
		shards, _, err := w.discoverer.DiscoverTargets(ctx)
		if err != nil {
			return JobResult{}, false, err
		}
		for _, s := range shards {
			w.queue.Enqueue(s)
		}
	}

	target, ok := w.queue.Dequeue()
	if !ok {
		return JobResult{}, false, nil
	}
	res, err := w.engine.CompactShard(ctx, target)
	return res, true, err
}

func (w *Worker) compactionLoop(ctx context.Context) {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		for {
			if ctx.Err() != nil {
				return
			}
			res, ran, err := w.StepCompaction(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if errors.Is(err, ErrLeaseContention) {
					w.logger.Debug("shard lease held by peer worker", "error", err)
				} else {
					w.logger.Error("compaction cycle failed", "error", err)
				}
			} else if ran && !res.Skipped {
				w.logger.Info("compaction shard completed",
					"tenant_id", string(res.Repo.TenantID),
					"repo_id", string(res.Repo.RepoID),
					"shard", res.Shard,
					"tier", string(res.Tier),
					"input_packs", res.InputPacks,
					"output_pack_id", res.OutputPackID,
					"bytes_reclaimed", res.BytesReclaimed,
				)
			}
			if !ran {
				break
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(w.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.discoverer == nil {
				continue
			}
			_, repos, err := w.discoverer.DiscoverTargets(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.logger.Error("gc target discovery failed", "error", err)
				continue
			}
			for _, repo := range repos {
				if ctx.Err() != nil {
					return
				}
				res, err := w.sweeper.SweepRepo(ctx, repo.Repo, repo.ManifestShards)
				if err != nil {
					w.logger.Error("delayed gc sweep failed",
						"tenant_id", string(repo.Repo.TenantID),
						"repo_id", string(repo.Repo.RepoID),
						"error", err,
					)
					continue
				}
				if res.CompactedReaped > 0 || res.OrphanedReaped > 0 {
					w.logger.Info("delayed gc sweep completed",
						"tenant_id", string(res.Repo.TenantID),
						"repo_id", string(res.Repo.RepoID),
						"compacted_reaped", res.CompactedReaped,
						"orphaned_reaped", res.OrphanedReaped,
						"bytes_reclaimed", res.BytesReclaimed,
					)
				}
			}
		}
	}
}
