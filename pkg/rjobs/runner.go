// Copyright 2018-2026 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package rjobs

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// defaultRetryAfter is how long a failed on-demand or leader run waits before
// it becomes claimable again.
const defaultRetryAfter = 30 * time.Second

// schedulerTick is how often the scheduler loop checks for due periodic jobs.
const schedulerTick = 10 * time.Second

// recoveryTick is how often the recovery loop looks for dedup placeholders
// stuck in the publish-pending phase of an enqueue whose process died.
const recoveryTick = 30 * time.Second

// pendingPublishGrace is how long a publish-pending reservation is trusted as
// an enqueue still in flight before recovery revokes it. It is well above the
// bounded publish request time, so a live enqueue is never revoked; a possible
// late message for a revoked row is dropped at adoption, never executed.
const pendingPublishGrace = time.Minute

// Options configures a Runner.
type Options struct {
	// Workers is the number of concurrent workers draining the durable queue.
	Workers int
	// Store is the durable backend. It may be nil, in which case only
	// all-nodes periodic jobs run (no leader-scoped or on-demand work).
	Store Store
	// Status records and serves per-run status. It is required whenever Store
	// is set, since every durable run gets a status record.
	Status StatusStore
	// OnDemandConfig holds the configuration of the on-demand jobs, keyed by
	// job name. When a run is dispatched the entry for its job is handed to the
	// job's constructor; a job with no entry is built with a nil map and falls
	// back to its own defaults.
	OnDemandConfig map[string]map[string]any
}

// Runner owns the scheduling, dispatching and execution of jobs. A process
// has a single Runner, created by the jobs service and reachable through
// Default for in-process Enqueue.
type Runner struct {
	workers        int
	store          Store
	status         StatusStore
	log            zerolog.Logger
	periodic       []Periodic
	onDemandConfig map[string]map[string]any

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// running guards against overlapping runs for jobs whose Overlap policy is
	// Skip.
	runningMu sync.Mutex
	running   map[string]bool

	// cancels holds the cancellation handle of every run currently executing on
	// this process, so a cancel request can interrupt the right run. A run is
	// added when execRun starts it and removed when it returns.
	cancelsMu sync.Mutex
	cancels   map[RunID]*runHandle
}

// runHandle is the per-run cancellation handle held while a run executes on
// this process. cancel stops the run's context; cancelled records that the stop
// was an explicit cancellation and not a shutdown, so execRun finalises the run
// as cancelled instead of retrying it. stale records that a newer delivery
// attempt took the run over, so this owner must stand down without forming any
// terminal state. once keeps the stop idempotent across the several places
// that may request it (a direct local cancel, the cancel broadcast, the
// backstop poll, the attempt-fence watcher).
type runHandle struct {
	job       string
	started   time.Time
	cancel    context.CancelFunc
	cancelled atomic.Bool
	stale     atomic.Bool
	once      sync.Once
}

// trip marks the run cancelled and cancels its context. Only the first stop
// request has an effect.
func (h *runHandle) trip() {
	h.once.Do(func() {
		h.cancelled.Store(true)
		h.cancel()
	})
}

// standDown marks the run lost to a newer attempt and cancels its context.
func (h *runHandle) standDown() {
	h.once.Do(func() {
		h.stale.Store(true)
		h.cancel()
	})
}

// NewRunner builds a Runner from the registered jobs and the given options.
func NewRunner(ctx context.Context, opts Options) (*Runner, error) {
	if opts.Workers <= 0 {
		opts.Workers = 4
	}

	r := &Runner{
		workers:        opts.Workers,
		store:          opts.Store,
		status:         opts.Status,
		log:            *appctx.GetLogger(ctx),
		periodic:       registeredPeriodic(),
		onDemandConfig: opts.OnDemandConfig,
		running:        make(map[string]bool),
		cancels:        make(map[RunID]*runHandle),
	}

	// leader-scoped and on-demand work both need a store.
	if r.store == nil {
		for _, p := range r.periodic {
			if p.Scope == ScopeLeader {
				return nil, errors.Errorf("rjobs: periodic job %q is leader-scoped but no store is configured", p.Name)
			}
		}
	}

	// a durable backend implies a status store: every queued/claimed run gets
	// a status record.
	if r.store != nil && r.status == nil {
		return nil, errors.New("rjobs: a status store is required when a durable store is configured")
	}

	return r, nil
}

// Start launches the runner loops. It returns immediately; the loops run until
// Stop is called.
func (r *Runner) Start() {
	ctx, cancel := context.WithCancel(appctx.WithLogger(context.Background(), &r.log))
	r.cancel = cancel

	// all-nodes periodic jobs run as local tickers, regardless of the store.
	for _, p := range r.periodic {
		if p.Scope == ScopeAllNodes {
			r.wg.Go(func() { r.runLocalTicker(ctx, p) })
		}
	}

	if r.store == nil {
		r.log.Info().Int("local_jobs", len(r.periodic)).Msg("rjobs: started without a store, only all-nodes jobs run")
		return
	}

	// subscribe to cluster-wide cancel broadcasts so a cancel issued on another
	// process can stop a run executing here. It is optional: without it cancels
	// still land via the backstop poll, just not instantly.
	if bus, ok := r.store.(ControlBus); ok {
		if err := bus.SubscribeCancel(ctx, r.tripLocal); err != nil {
			r.log.Error().Err(err).Msg("rjobs: subscribing to cancel broadcasts failed, cancels fall back to the backstop poll")
		}
	}

	// register leader-scoped schedules so the scheduler can track them.
	for _, p := range r.periodic {
		if p.Scope != ScopeLeader {
			continue
		}
		sched, _ := ParseSchedule(p.Schedule) // validated at registration
		next := time.Now().Add(sched.Interval())
		if p.RunOnStart {
			next = time.Now()
		}
		if err := r.store.RegisterScheduled(ctx, p.Name, sched, next, p.RunOnStart); err != nil {
			r.log.Error().Err(err).Str("job", p.Name).Msg("rjobs: registering schedule failed")
		}
	}

	r.wg.Go(func() { r.runScheduler(ctx) })
	r.wg.Go(func() { r.runRecovery(ctx) })

	for i := 0; i < r.workers; i++ {
		r.wg.Go(func() { r.runDispatcher(ctx) })
	}

	r.log.Info().Int("workers", r.workers).Msg("rjobs: runner started")
}

// Stop cancels the loops and waits for in-flight work to drain, bounded by
// ctx.
func (r *Runner) Stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		r.log.Warn().Msg("rjobs: shutdown deadline reached with work still in flight")
	}

	if r.store != nil {
		if err := r.store.Close(ctx); err != nil {
			return err
		}
	}
	if r.status != nil {
		if err := r.status.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue schedules an on-demand job to run as soon as a worker is free. It is
// durable: it returns once the run is persisted, so the run survives a restart
// of this replica. This is the in-process trigger API; a future RPC service
// can wrap it without the core changing.
func (r *Runner) Enqueue(ctx context.Context, name string, p Params, opts ...EnqueueOption) (RunID, error) {
	if r.store == nil {
		return "", errors.New("rjobs: cannot enqueue, no store configured")
	}
	if _, ok := lookupOnDemand(name); !ok {
		return "", errors.Errorf("rjobs: no on-demand job %q registered", name)
	}

	run := Run{Job: name, Params: p}
	for _, o := range opts {
		o(&run)
	}

	// Fix the stable identity before any write, so the queue message, the
	// status record and the dedup placeholder all refer to the same run and a
	// crashed commit phase can be resumed against that identity instead of
	// spawning a second run.
	run.ID = RunID(uuid.NewString())
	run.Attempt = 1
	run.EnqueuedAt = time.Now()

	if run.DedupKey != "" {
		return r.enqueueUnique(ctx, run)
	}

	id, err := r.store.Enqueue(ctx, run)
	if err != nil {
		return "", err
	}

	// Enqueue without a dedup key keeps its original best-effort semantics:
	// the message is the durable fact, and a failed status write does not fail
	// the enqueue. The gap is healed when the message is claimed — Adopt
	// rebuilds the original run's status from the identity carried by the
	// message, so the run never executes unknown and becomes queryable.
	if err := r.status.Put(ctx, Status{
		RunID:      id,
		Job:        name,
		State:      StateQueued,
		Attempt:    1,
		EnqueuedAt: run.EnqueuedAt,
		Owner:      run.Owner,
	}); err != nil {
		r.log.Error().Err(err).Str("run", string(id)).Msg("rjobs: recording queued status failed; the run is rebuilt at claim")
	}
	return id, nil
}

// enqueueUnique enqueues a run that carries a Unique key through a recoverable
// commit phase: (1) Reserve inserts the queued row and the dedup placeholder in
// the publish-pending phase; (2) the queue message is published; (3)
// MarkCommitted closes the phase. On conflict it collapses onto the in-flight
// run (default) or rejects, per the run's policy. If the publish fails the
// placeholder is revoked, so the key is never stuck; should the process die
// between (1) and (3), the recovery loop revokes the pending placeholder after
// a grace period. A message that nonetheless reached the queue for a revoked
// row is recognised as obsolete when claimed and never executes.
func (r *Runner) enqueueUnique(ctx context.Context, run Run) (RunID, error) {
	queued := Status{
		RunID:      run.ID,
		Job:        run.Job,
		State:      StateQueued,
		Attempt:    1,
		EnqueuedAt: run.EnqueuedAt,
		Owner:      run.Owner,
	}

	existing, reserved, err := r.status.Reserve(ctx, queued, run.DedupKey)
	if err != nil {
		return "", err
	}
	if !reserved {
		if run.dedupReject {
			return "", errtypes.AlreadyExists(fmt.Sprintf("run %s already active for key %q", existing.RunID, run.DedupKey))
		}
		return existing.RunID, nil
	}

	if _, err := r.store.Enqueue(ctx, run); err != nil {
		// The placeholder exists but publication failed: revoke it so the key
		// is freed and the run is a failed record instead of a phantom queued
		// row that would hold the key forever. The marker is kept, so a
		// message the server may still have accepted is dropped at claim.
		if rerr := r.status.Revoke(ctx, run.ID, errors.Wrap(err, "publishing run failed").Error()); rerr != nil {
			r.log.Error().Err(rerr).Str("run", string(run.ID)).Msg("rjobs: revoking reservation after failed publish errored")
		}
		return "", err
	}

	// Publication is durable; leave the commit phase. If this write fails the
	// row merely looks publish-pending: either the next claim clears the marker
	// (the message physically exists) or the recovery loop revokes it, in which
	// case the message is obsolete and is acked without running.
	if err := r.status.MarkCommitted(ctx, run.ID); err != nil {
		r.log.Error().Err(err).Str("run", string(run.ID)).Msg("rjobs: marking enqueue committed failed; recovery will reconcile")
	}
	return run.ID, nil
}

// TriggerNow enqueues an immediate, out-of-band run of a leader-scoped periodic
// job, on top of its schedule: the regular cadence is left untouched, so this is
// an extra run, not a reschedule. It respects the job's single-flight guard, so
// it is rejected with an error if a run of the job is already in flight. Like
// Enqueue it is in-process; on-demand and all-nodes jobs cannot be triggered.
func (r *Runner) TriggerNow(ctx context.Context, job string) error {
	if r.store == nil {
		return errors.New("rjobs: cannot trigger, no store configured")
	}
	if !r.isLeaderJob(job) {
		return errors.Errorf("rjobs: %q is not a registered leader periodic job", job)
	}

	acquired, err := r.store.TryMarkScheduledRunning(ctx, job)
	if err != nil {
		return err
	}
	if !acquired {
		return errors.Errorf("rjobs: a run of %q is already in flight", job)
	}

	if _, err := r.store.Enqueue(ctx, Run{Job: job}); err != nil {
		// release the gate we just took so the job is not stuck marked-running.
		if cerr := r.store.ClearScheduledRunning(ctx, job); cerr != nil {
			r.log.Error().Err(cerr).Str("job", job).Msg("rjobs: releasing schedule mark after a failed trigger errored")
		}
		return errors.Wrap(err, "rjobs: enqueuing triggered run failed")
	}
	r.log.Info().Str("job", job).Msg("rjobs: triggered an immediate run")
	return nil
}

// Status returns the current status of a previously enqueued run. It returns
// an errtypes.NotFound error if the run is unknown.
func (r *Runner) Status(ctx context.Context, id RunID) (Status, error) {
	if r.status == nil {
		return Status{}, errors.New("rjobs: no status store configured")
	}
	return r.status.Get(ctx, id)
}

// ListByOwner returns the runs created for the given user, most recently
// enqueued first. It is the read side of WithOwner: a UI lists a user's jobs
// with it. The filter narrows the result further (by state, job or page); its
// Owner and Internal fields are ignored — the owner argument wins and internal
// runs are never included.
func (r *Runner) ListByOwner(ctx context.Context, owner string, f ListFilter) ([]Status, error) {
	if r.status == nil {
		return nil, errors.New("rjobs: no status store configured")
	}
	f.Owner = owner
	f.Internal = false
	return r.status.List(ctx, f)
}

// Cancel requests cancellation of a previously enqueued run and returns its
// updated status. Cancellation is cooperative and asynchronous: it records a
// durable cancel intent and stops the run on this process if it is running
// here. The run reaches StateCancelled once it actually stops, so callers
// observe Status to confirm. A run that ignores its context runs to completion.
// Cancelling a finished run is a no-op; it returns an errtypes.NotFound error
// if the run is unknown.
func (r *Runner) Cancel(ctx context.Context, id RunID) (Status, error) {
	if r.status == nil {
		return Status{}, errors.New("rjobs: no status store configured")
	}
	// Durable intent first: it is the source of truth and the backstop should
	// the fast local path not reach the worker running the job.
	st, err := r.status.RequestCancel(ctx, id)
	if err != nil {
		return Status{}, err
	}
	// Fast path: stop it now if it runs here, and broadcast so a worker running
	// it on another process stops without waiting for the backstop poll.
	r.tripLocal(CancelSignal{RunID: id})
	r.broadcastCancel(ctx, CancelSignal{RunID: id})
	return st, nil
}

// CancelPeriodic requests cancellation of the in-flight run of a leader-scoped
// periodic job, addressed by job name (a leader job has at most one run in
// flight). Like Cancel it is cooperative and asynchronous. The schedule itself
// is untouched, so the job keeps firing on its normal cadence; this stops only
// the current run. It returns an error if the job is not a registered leader
// job or if no run of it is currently in flight.
func (r *Runner) CancelPeriodic(ctx context.Context, job string) error {
	if r.store == nil {
		return errors.New("rjobs: cannot cancel, no store configured")
	}
	if !r.isLeaderJob(job) {
		return errors.Errorf("rjobs: %q is not a registered leader periodic job", job)
	}

	running, err := r.store.RequestCancelScheduled(ctx, job)
	if err != nil {
		return err
	}
	if !running {
		return errors.Errorf("rjobs: no run of %q is currently in flight", job)
	}

	// Fast path: stop it here if it runs on this process, and broadcast to the
	// process running it otherwise.
	r.tripLocal(CancelSignal{Job: job})
	r.broadcastCancel(ctx, CancelSignal{Job: job})
	return nil
}

// registerRun records a run's cancellation handle for the duration of its
// execution on this process.
func (r *Runner) registerRun(id RunID, h *runHandle) {
	r.cancelsMu.Lock()
	r.cancels[id] = h
	r.cancelsMu.Unlock()
}

// deregisterRun drops a run's cancellation handle once it has finished.
func (r *Runner) deregisterRun(id RunID) {
	r.cancelsMu.Lock()
	delete(r.cancels, id)
	r.cancelsMu.Unlock()
}

// tripLocal stops any run matching sig that is executing on this process: a run
// by id (on-demand) or every in-flight run of a job (periodic). It is the
// node-local half of a cancel, invoked directly and from the cancel broadcast,
// and a no-op when no matching run is running here.
func (r *Runner) tripLocal(sig CancelSignal) {
	r.cancelsMu.Lock()
	defer r.cancelsMu.Unlock()
	if sig.RunID != "" {
		if h, ok := r.cancels[sig.RunID]; ok {
			h.trip()
		}
		return
	}
	if sig.Job == "" {
		return
	}
	for _, h := range r.cancels {
		if h.job == sig.Job {
			h.trip()
		}
	}
}

// broadcastCancel best-effort publishes a cancel signal to the other processes
// when the store supports it, so a run executing elsewhere stops without waiting
// for its backstop poll. A failure only falls back to that poll, so it is
// logged, not returned.
func (r *Runner) broadcastCancel(ctx context.Context, sig CancelSignal) {
	bus, ok := r.store.(ControlBus)
	if !ok {
		return
	}
	if err := bus.PublishCancel(ctx, sig); err != nil {
		r.log.Warn().Err(err).Msg("rjobs: broadcasting cancel failed; relying on the backstop poll")
	}
}

// runLocalTicker drives an all-nodes periodic job on this replica. It never
// touches the store.
func (r *Runner) runLocalTicker(ctx context.Context, p Periodic) {
	sched, _ := ParseSchedule(p.Schedule)
	log := r.log.With().Str("job", p.Name).Str("scope", p.Scope.String()).Logger()

	if p.RunOnStart {
		r.execPeriodic(ctx, p, log)
	}

	for {
		wait := sched.Interval() + jitter(p.Jitter)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
			r.execPeriodic(ctx, p, log)
		}
	}
}

// runScheduler enqueues due leader-scoped periodic runs. Every jobs process
// runs this loop: the store's DueScheduled atomically advances each job's
// next-fire, so exactly one process wins a given tick and enqueues it. No
// leader election is needed.
func (r *Runner) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(schedulerTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			due, err := r.store.DueScheduled(ctx, time.Now())
			if err != nil {
				r.log.Error().Err(err).Msg("rjobs: querying due jobs failed")
				continue
			}
			for _, d := range due {
				// The store may surface a schedule entry for a job that is no
				// longer a registered leader job: its scope changed to
				// all-nodes, or it was deleted/renamed, leaving a stale entry
				// in the store. Skip it here rather than enqueuing a run that
				// would double-execute (all-nodes) or reference a job that no
				// longer exists. The stale entry is left in place; it is
				// harmless once it is never enqueued.
				if !r.isLeaderJob(d.Job) {
					r.log.Debug().Str("job", d.Job).Msg("rjobs: skipping stale schedule entry, not a registered leader job")
					continue
				}
				if _, err := r.store.Enqueue(ctx, Run{Job: d.Job}); err != nil {
					r.log.Error().Err(err).Str("job", d.Job).Msg("rjobs: enqueuing periodic run failed")
					continue
				}
				// mark the job as in flight so the scheduler does not enqueue
				// another run until this one finishes (see execRun, which
				// clears it).
				if err := r.store.MarkScheduledRunning(ctx, d.Job); err != nil {
					r.log.Error().Err(err).Str("job", d.Job).Msg("rjobs: marking schedule running failed")
				}
			}
		}
	}
}

// runDispatcher claims runs from the store and executes them.
func (r *Runner) runDispatcher(ctx context.Context) {
	for {
		run, err := r.store.Claim(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.log.Error().Err(err).Msg("rjobs: claiming run failed")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		r.execRun(ctx, run)
	}
}

// execRun dispatches a claimed durable run. Leader periodic runs keep their
// non-status, schedule-gated semantics; on-demand runs go through the
// attempt-fenced status adoption path.
func (r *Runner) execRun(ctx context.Context, run Run) {
	if _, ok := r.lookupPeriodic(run.Job); ok {
		r.execPeriodicRun(ctx, run)
		return
	}
	r.execOnDemandRun(ctx, run)
}

// execPeriodicRun runs a claimed leader-scoped periodic run. Periodic runs are
// not status-tracked: their cancel intent lives in the schedule store and
// their retry semantics are the queue's plain nak/redeliver. While the job runs
// it heartbeats so a long run keeps its claim and is not redelivered.
func (r *Runner) execPeriodicRun(ctx context.Context, run Run) {
	log := r.log.With().Str("job", run.Job).Str("run", string(run.ID)).Int("attempt", run.Attempt).Logger()

	// once this run finishes, the job is no longer in flight, so the scheduler
	// may schedule it again. A run dropped by the overlap guard is the
	// exception: it never owned the schedule mark, so the still-running owner
	// must clear it, not this run.
	var skippedOverlap bool
	defer func() {
		if skippedOverlap {
			return
		}
		if err := r.store.ClearScheduledRunning(ctx, run.Job); err != nil {
			log.Error().Err(err).Msg("rjobs: clearing schedule running mark failed")
		}
	}()

	// Per-run cancellable context, registered so a cancel request can interrupt
	// this specific run. It derives from ctx, so a shutdown still cancels it too;
	// the runHandle.cancelled flag is what tells the two apart below.
	runCtx, cancel := context.WithCancel(ctx)
	h := &runHandle{job: run.Job, started: time.Now(), cancel: cancel}
	r.registerRun(run.ID, h)
	defer r.deregisterRun(run.ID)
	defer cancel()

	// A cancellation may have arrived while the run was queued or waiting to be
	// retried. Honour it before doing any work: drop the run as cancelled
	// instead of executing it.
	if r.periodicCancelRequested(ctx, run) {
		log.Info().Msg("rjobs: run cancelled before it started")
		r.completeCancelled(ctx, run, log)
		return
	}

	stopBeat := r.startHeartbeat(ctx, run, h, log)
	defer stopBeat()

	_, err := r.invoke(runCtx, run, log)

	// An explicit cancellation wins over the job's return value: the run is
	// terminal and must not be retried, whether the job returned an error or
	// not. A shutdown does not set this flag, so in-flight work is still
	// retried after a restart.
	if h.cancelled.Load() {
		log.Info().Msg("rjobs: run cancelled")
		r.completeCancelled(ctx, run, log)
		return
	}

	// An overlap-skip is an expected outcome, not a failure: ack and drop
	// without a retry.
	if errors.Is(err, errSkippedOverlap) {
		skippedOverlap = true
		log.Debug().Msg("rjobs: run skipped, previous run still in flight")
		if cerr := r.store.Complete(ctx, run.ID); cerr != nil {
			log.Error().Err(cerr).Msg("rjobs: completing skipped run errored")
		}
		return
	}

	if err != nil {
		log.Error().Err(err).Msg("rjobs: run failed")
		if ferr := r.store.Fail(ctx, run.ID, defaultRetryAfter); ferr != nil {
			log.Error().Err(ferr).Msg("rjobs: marking run failed errored")
		}
		return
	}

	if cerr := r.store.Complete(ctx, run.ID); cerr != nil {
		log.Error().Err(cerr).Msg("rjobs: completing run errored")
	}
}

// execOnDemandRun runs a claimed on-demand run behind the status-store attempt
// fence. Adoption first reconciles the delivery with the durable status: it
// rebuilds a missing row, hands the run to its current attempt, resubmits a
// result an earlier attempt died while committing, or rejects an obsolete
// delivery. From then on every lifecycle write is conditioned on this attempt,
// so a redelivery after a heartbeat timeout and an old owner's late write can
// neither re-run a completed job nor overwrite the current attempt.
func (r *Runner) execOnDemandRun(ctx context.Context, run Run) {
	log := r.log.With().Str("job", run.Job).Str("run", string(run.ID)).Int("attempt", run.Attempt).Logger()

	runCtx, cancel := context.WithCancel(ctx)
	h := &runHandle{job: run.Job, started: time.Now(), cancel: cancel}
	r.registerRun(run.ID, h)
	defer r.deregisterRun(run.ID)
	defer cancel()

	// Reconcile the delivery with the durable status. The skeleton is the
	// stable identity carried by the queue message; it rebuilds the original
	// run's state when the enqueue's status write was lost.
	skeleton := Status{
		RunID:      run.ID,
		Job:        run.Job,
		Attempt:    run.Attempt,
		EnqueuedAt: run.EnqueuedAt,
		Owner:      run.Owner,
	}
	decision, st, err := r.status.Adopt(ctx, skeleton, run.Attempt)
	if err != nil {
		// Without the fence we must neither run nor ack: leave the delivery to
		// time out and be redelivered, when adoption is retried.
		log.Error().Err(err).Msg("rjobs: adopting claimed run failed; leaving it for redelivery")
		return
	}
	if decision == AdoptObsolete {
		// terminal run, revoked placeholder, duplicate delivery or stale
		// attempt: ack and do nothing — no unknown message may execute.
		log.Info().Str("state", string(st.State)).Msg("rjobs: obsolete delivery acked without running")
		if cerr := r.store.Complete(ctx, run.ID); cerr != nil {
			log.Error().Err(cerr).Msg("rjobs: acking obsolete delivery errored")
		}
		return
	}
	if st.StartedAt != nil {
		h.started = *st.StartedAt
	}

	// A cancellation that landed while the run was queued wins before any work.
	if st.CancelRequested {
		log.Info().Msg("rjobs: run cancelled before it started")
		r.finishCancelledFenced(ctx, run, st, log)
		return
	}

	stopBeat := r.startHeartbeat(ctx, run, h, log)
	defer stopBeat()

	var result Params
	if decision == AdoptResubmit {
		// An earlier attempt already produced and staged this result but died
		// before the terminal commit. Hand the existing result back in instead
		// of running the job again.
		result = st.Result
		log.Info().Msg("rjobs: resubmitting result staged by an earlier attempt")
	} else {
		result, err = r.invoke(runCtx, run, log)

		// A newer attempt took over while the job ran (its redelivery was
		// already claimed): stand down without any terminal write or ack.
		if h.stale.Load() {
			log.Info().Msg("rjobs: a newer attempt took over; standing down")
			return
		}
		// An explicit cancellation wins over the job's return value. A shutdown
		// does not set the flag, so in-flight work is retried.
		if h.cancelled.Load() {
			log.Info().Msg("rjobs: run cancelled")
			r.finishCancelledFenced(ctx, run, st, log)
			return
		}
		if err != nil {
			r.failAttempt(ctx, run, err, log)
			return
		}

		// Persist the result while still non-terminal. If the process dies
		// after this and before the terminal commit, the redelivery adopts
		// AdoptResubmit and commits this same result instead of re-running.
		if serr := r.status.StageResult(ctx, run.ID, run.Attempt, result); serr != nil {
			// A lost fence makes the commit a no-op anyway; a transient store
			// error may still be salvaged by the commit carrying the result.
			log.Warn().Err(serr).Msg("rjobs: staging result failed")
		}
	}

	committed, cerr := r.commitTerminal(ctx, run, st.StartedAt, result, StateSucceeded, "", log)
	if cerr != nil {
		// The terminal state is not durable and the message is not acked: the
		// lease lapses and the redelivery resubmits the staged result.
		log.Error().Err(cerr).Msg("rjobs: committing succeeded state failed; redelivery will resubmit")
		return
	}
	if !committed {
		log.Info().Msg("rjobs: another attempt owns the run; not completing")
		return
	}

	r.releaseReservation(ctx, run, log)
	if err := r.store.Complete(ctx, run.ID); err != nil {
		log.Error().Err(err).Msg("rjobs: completing run errored")
	}
}

// failAttempt records the failed attempt behind the fence and naks for the
// ordinary retry. Losing the fence (a newer attempt already owns the run) or a
// store error leaves the delivery unacked rather than retrying someone else's
// attempt.
func (r *Runner) failAttempt(ctx context.Context, run Run, jobErr error, log zerolog.Logger) {
	log.Error().Err(jobErr).Msg("rjobs: run failed")
	committed, err := r.status.MarkFailed(ctx, run.ID, run.Attempt, jobErr.Error(), time.Now())
	if err != nil {
		log.Error().Err(err).Msg("rjobs: recording failed attempt failed; leaving the run to redeliver")
		return
	}
	if !committed {
		log.Info().Msg("rjobs: another attempt owns the run; not failing it")
		return
	}
	if ferr := r.store.Fail(ctx, run.ID, defaultRetryAfter); ferr != nil {
		log.Error().Err(ferr).Msg("rjobs: marking run failed errored")
	}
}

// commitTerminal writes a terminal status behind the attempt fence. Only the
// writer that still owns the attempt forms the terminal state, so a completed
// run cannot be resurrected by a late cancel and an old owner cannot overwrite
// a newer attempt.
func (r *Runner) commitTerminal(ctx context.Context, run Run, started *time.Time, result Params, state State, lastError string, log zerolog.Logger) (bool, error) {
	now := time.Now()
	st := Status{
		RunID:      run.ID,
		Job:        run.Job,
		State:      state,
		Attempt:    run.Attempt,
		EnqueuedAt: run.EnqueuedAt,
		StartedAt:  started,
		FinishedAt: &now,
		LastError:  lastError,
		Result:     result,
		Owner:      run.Owner,
	}
	committed, err := r.status.CommitTerminal(ctx, st)
	if err != nil {
		return false, err
	}
	if !committed {
		return false, nil
	}
	return true, nil
}

// finishCancelledFenced commits the cancelled terminal state behind the
// attempt fence and acks. A fence loss (newer attempt or the run already
// terminal) stands down silently.
func (r *Runner) finishCancelledFenced(ctx context.Context, run Run, adopted Status, log zerolog.Logger) {
	committed, err := r.commitTerminal(ctx, run, adopted.StartedAt, nil, StateCancelled, "", log)
	if err != nil {
		log.Error().Err(err).Msg("rjobs: committing cancelled state failed; leaving the run to redeliver")
		return
	}
	if !committed {
		log.Info().Msg("rjobs: another attempt owns the run; not cancelling it")
		return
	}
	r.releaseReservation(ctx, run, log)
	if err := r.store.Complete(ctx, run.ID); err != nil {
		log.Error().Err(err).Msg("rjobs: completing cancelled run errored")
	}
}

// completeCancelled acks a cancelled periodic run. Periodic runs are not
// status-tracked, so cancellation is terminal through the ack alone.
func (r *Runner) completeCancelled(ctx context.Context, run Run, log zerolog.Logger) {
	if err := r.store.Complete(ctx, run.ID); err != nil {
		log.Error().Err(err).Msg("rjobs: completing cancelled run errored")
	}
}

// releaseReservation frees a successful/cancelled unique run's dedup key. It
// is a no-op for runs without a key.
func (r *Runner) releaseReservation(ctx context.Context, run Run, log zerolog.Logger) {
	if run.DedupKey == "" {
		return
	}
	if err := r.status.Release(ctx, run.ID); err != nil {
		log.Error().Err(err).Msg("rjobs: releasing dedup reservation errored")
	}
}

// periodicCancelRequested reports the durable cancel intent for a periodic
// run, which lives in the schedule store keyed by job name.
func (r *Runner) periodicCancelRequested(ctx context.Context, run Run) bool {
	req, err := r.store.ScheduledCancelRequested(ctx, run.Job)
	if err != nil {
		return false
	}
	return req
}

// startHeartbeat keeps a claimed run's lease alive by calling the store's
// Heartbeat on a ticker until the returned stop function is called. It lets a
// job run for arbitrarily long (minutes or days) without being redelivered.
// The tick is also the backstop watcher: for on-demand runs it notices both a
// durable cancel intent and a newer attempt that took over, for periodic runs
// the schedule-store cancel intent.
func (r *Runner) startHeartbeat(ctx context.Context, run Run, h *runHandle, log zerolog.Logger) (stop func()) {
	interval := r.store.HeartbeatInterval()
	if interval <= 0 {
		return func() {}
	}

	_, periodic := r.lookupPeriodic(run.Job)

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.store.Heartbeat(ctx, run.ID); err != nil {
					log.Warn().Err(err).Msg("rjobs: heartbeat failed")
				}
				if h.cancelled.Load() || h.stale.Load() {
					continue
				}
				if periodic {
					if r.periodicCancelRequested(ctx, run) {
						log.Info().Msg("rjobs: cancellation observed, stopping run")
						h.trip()
					}
					continue
				}
				// On-demand: a missing/unreadable status must not stop the run;
				// the lease simply keeps being extended.
				st, err := r.status.Get(ctx, run.ID)
				if err != nil {
					continue
				}
				switch {
				case st.Attempt > run.Attempt:
					// The lease lapsed and a newer delivery adopted the run:
					// this old owner stops so its late writes cannot win.
					log.Info().Int("new_attempt", st.Attempt).Msg("rjobs: a newer attempt took over; standing down")
					h.standDown()
				case st.CancelRequested:
					log.Info().Msg("rjobs: cancellation observed, stopping run")
					h.trip()
				}
			}
		}
	}()

	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// runRecovery reconciles enqueues whose commit phase was interrupted. On
// startup it runs immediately, so a restarted process advances the original
// phase instead of recreating or running the job, and then periodically revokes
// dedup placeholders whose publish was never confirmed. Claim-time adoption
// covers the other two interrupted phases (a missing status row and an
// uncommitted terminal result).
func (r *Runner) runRecovery(ctx context.Context) {
	sweeper, ok := r.status.(interface {
		RevokePending(ctx context.Context, grace time.Duration) ([]RunID, error)
	})
	if !ok {
		return
	}

	sweep := func() {
		ids, err := sweeper.RevokePending(ctx, pendingPublishGrace)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error().Err(err).Msg("rjobs: recovering pending reservations failed")
			}
			return
		}
		for _, id := range ids {
			r.log.Warn().Str("run", string(id)).Msg("rjobs: revoked a reservation whose publish was never confirmed")
		}
	}

	sweep()
	ticker := time.NewTicker(recoveryTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

// invoke dispatches a run to the right registered job. Periodic runs (empty
// Params and a known periodic name) call the periodic Run closure; everything
// else is an on-demand job. The returned Params are the on-demand job's result
// (always nil for periodic runs).
func (r *Runner) invoke(ctx context.Context, run Run, log zerolog.Logger) (Params, error) {
	jobCtx := appctx.WithLogger(ctx, &log)

	if p, ok := r.lookupPeriodic(run.Job); ok {
		return nil, r.guard(p, func() error { return p.Run(jobCtx) })
	}

	newFunc, ok := lookupOnDemand(run.Job)
	if !ok {
		return nil, errors.Errorf("rjobs: run references unknown job %q", run.Job)
	}
	job, err := newFunc(jobCtx, r.onDemandConfig[run.Job])
	if err != nil {
		return nil, errors.Wrap(err, "rjobs: building on-demand job failed")
	}
	return job.Run(jobCtx, run.Params)
}

// execPeriodic runs a periodic Run closure inline (used by the local ticker).
func (r *Runner) execPeriodic(ctx context.Context, p Periodic, log zerolog.Logger) {
	start := time.Now()
	err := r.guard(p, func() error { return p.Run(appctx.WithLogger(ctx, &log)) })
	switch {
	case errors.Is(err, errSkippedOverlap):
		log.Debug().Msg("rjobs: skipped, previous run still in flight")
	case err != nil:
		log.Error().Err(err).Dur("took", time.Since(start)).Msg("rjobs: periodic run failed")
	default:
		log.Debug().Dur("took", time.Since(start)).Msg("rjobs: periodic run done")
	}
}

var errSkippedOverlap = errors.New("rjobs: run skipped due to overlap policy")

// guard enforces the overlap policy for a periodic job.
func (r *Runner) guard(p Periodic, fn func() error) error {
	if p.Overlap == Allow {
		return fn()
	}

	r.runningMu.Lock()
	if r.running[p.Name] {
		r.runningMu.Unlock()
		return errSkippedOverlap
	}
	r.running[p.Name] = true
	r.runningMu.Unlock()

	defer func() {
		r.runningMu.Lock()
		r.running[p.Name] = false
		r.runningMu.Unlock()
	}()

	return fn()
}

func (r *Runner) lookupPeriodic(name string) (Periodic, bool) {
	for _, p := range r.periodic {
		if p.Name == name {
			return p, true
		}
	}
	return Periodic{}, false
}

// isLeaderJob reports whether name is currently registered as a leader-scoped
// periodic job. The scheduler uses it to ignore stale schedule entries left by
// a job that changed scope or was removed.
func (r *Runner) isLeaderJob(name string) bool {
	p, ok := r.lookupPeriodic(name)
	return ok && p.Scope == ScopeLeader
}

// jitter returns a random non-negative duration in [0, max).
func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}
