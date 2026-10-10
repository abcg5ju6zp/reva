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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cs3org/reva/v3/pkg/errtypes"
)

func TestAllNodesRunOnStart(t *testing.T) {
	resetRegistry()

	var runs int32
	done := make(chan struct{}, 1)
	err := RegisterPeriodic(Periodic{
		Name:       "test.warm",
		Schedule:   "@every 1h",
		Scope:      ScopeAllNodes,
		RunOnStart: true,
		Run: func(ctx context.Context) error {
			if atomic.AddInt32(&runs, 1) == 1 {
				done <- struct{}{}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	r, err := NewRunner(context.Background(), Options{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunOnStart job did not run")
	}
}

func TestLeaderJobNeedsStore(t *testing.T) {
	resetRegistry()

	if err := RegisterPeriodic(Periodic{
		Name:     "test.cleanup",
		Schedule: "@every 1h",
		Scope:    ScopeLeader,
		Run:      func(ctx context.Context) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := NewRunner(context.Background(), Options{}); err == nil {
		t.Fatal("expected error: leader job without a store")
	}
}

func TestStoreRequiresStatusStore(t *testing.T) {
	resetRegistry()

	if _, err := NewRunner(context.Background(), Options{Store: stubStore{}}); err == nil {
		t.Fatal("expected error: store configured without a status store")
	}
}

// stubStore is a minimal Store used only to exercise runner construction.
type stubStore struct{}

func (stubStore) Enqueue(context.Context, Run) (RunID, error)      { return "", nil }
func (stubStore) Claim(context.Context) (Run, error)               { return Run{}, nil }
func (stubStore) Complete(context.Context, RunID) error            { return nil }
func (stubStore) Fail(context.Context, RunID, time.Duration) error { return nil }
func (stubStore) Heartbeat(context.Context, RunID) error           { return nil }
func (stubStore) HeartbeatInterval() time.Duration                 { return 0 }
func (stubStore) DueScheduled(context.Context, time.Time) ([]ScheduledRun, error) {
	return nil, nil
}
func (stubStore) RegisterScheduled(context.Context, string, Schedule, time.Time, bool) error {
	return nil
}
func (stubStore) MarkScheduledRunning(context.Context, string) error  { return nil }
func (stubStore) ClearScheduledRunning(context.Context, string) error { return nil }
func (stubStore) TryMarkScheduledRunning(context.Context, string) (bool, error) {
	return false, nil
}
func (stubStore) RequestCancelScheduled(context.Context, string) (bool, error) {
	return false, nil
}
func (stubStore) ScheduledCancelRequested(context.Context, string) (bool, error) {
	return false, nil
}
func (stubStore) Close(context.Context) error { return nil }

func TestGuardSkipsOverlap(t *testing.T) {
	r := &Runner{running: make(map[string]bool)}
	p := Periodic{Name: "j", Overlap: Skip}

	block := make(chan struct{})
	go func() {
		_ = r.guard(p, func() error {
			<-block
			return nil
		})
	}()

	// wait for the first run to mark itself running
	for {
		r.runningMu.Lock()
		running := r.running["j"]
		r.runningMu.Unlock()
		if running {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if err := r.guard(p, func() error { return nil }); err != errSkippedOverlap {
		t.Fatalf("expected overlap skip, got %v", err)
	}
	close(block)
}

func TestIsLeaderJob(t *testing.T) {
	r := &Runner{
		periodic: []Periodic{
			{Name: "cleanup", Scope: ScopeLeader},
			{Name: "warm", Scope: ScopeAllNodes},
		},
	}

	if !r.isLeaderJob("cleanup") {
		t.Error("a registered leader job should be reported as such")
	}
	// a job that flipped to all-nodes must not be treated as leader, so a
	// stale schedule entry for it is skipped instead of double-running.
	if r.isLeaderJob("warm") {
		t.Error("an all-nodes job must not be reported as a leader job")
	}
	// a job no longer registered at all (deleted/renamed) is not a leader job.
	if r.isLeaderJob("gone") {
		t.Error("an unregistered job must not be reported as a leader job")
	}
}

// funcJob adapts a plain function to the Job interface for tests.
type funcJob func(ctx context.Context, p Params) (Params, error)

func (f funcJob) Run(ctx context.Context, p Params) (Params, error) { return f(ctx, p) }

func TestInvokePassesOnDemandConfig(t *testing.T) {
	resetRegistry()

	got := make(map[string]map[string]any)
	register := func(name string) {
		err := RegisterOnDemand(name, func(ctx context.Context, m map[string]any) (Job, error) {
			got[name] = m
			return funcJob(func(ctx context.Context, p Params) (Params, error) { return nil, nil }), nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	register("test.configured")
	register("test.bare")

	r, err := NewRunner(context.Background(), Options{
		OnDemandConfig: map[string]map[string]any{
			"test.configured": {"greeting": "hello"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := r.invoke(context.Background(), Run{Job: "test.configured"}, r.log); err != nil {
		t.Fatal(err)
	}
	if _, err := r.invoke(context.Background(), Run{Job: "test.bare"}, r.log); err != nil {
		t.Fatal(err)
	}

	if g := got["test.configured"]["greeting"]; g != "hello" {
		t.Errorf("configured job did not receive its config section: got %v", got["test.configured"])
	}
	// a job with no config section is built with a nil map, not an empty one,
	// so it can tell "unset" apart and rely on its own defaults.
	if got["test.bare"] != nil {
		t.Errorf("a job without a config section should receive nil, got %v", got["test.bare"])
	}
}

func resetRegistry() {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.periodic = make(map[string]Periodic)
	reg.onDemand = make(map[string]NewJob)
}

// jobFunc adapts a function to the Job interface for tests.
type jobFunc func(context.Context, Params) (Params, error)

func (f jobFunc) Run(ctx context.Context, p Params) (Params, error) { return f(ctx, p) }

// fakeStatus is an in-memory StatusStore that mirrors the SQL store's
// ownership rules: lifecycle writes are conditioned on the attempt, a Put never
// clears the cancel intent, the dedup key or the commit-phase marker, and
// reservations enforce one active run per (owner, key).
type fakeStatus struct {
	mu      sync.Mutex
	rec     map[RunID]Status
	pending map[RunID]time.Time // publish-pending marker
	keys    map[RunID]string    // active dedup key held per run

	// commitFailFor makes CommitTerminal return an error for a run at the
	// configured attempt, to simulate a terminal save failing before
	// redelivery.
	commitFailFor map[RunID]int
}

func newFakeStatus() *fakeStatus {
	return &fakeStatus{
		rec:           make(map[RunID]Status),
		pending:       make(map[RunID]time.Time),
		keys:          make(map[RunID]string),
		commitFailFor: make(map[RunID]int),
	}
}

func (f *fakeStatus) Put(_ context.Context, s Status) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.rec[s.RunID]; ok {
		// owned by other methods: never clobbered by a plain Put.
		s.CancelRequested = cur.CancelRequested
	}
	f.rec[s.RunID] = s
	return nil
}

func (f *fakeStatus) Get(_ context.Context, id RunID) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.rec[id]
	if !ok {
		return Status{}, errtypes.NotFound(string(id))
	}
	return s, nil
}

func (f *fakeStatus) RequestCancel(_ context.Context, id RunID) (Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.rec[id]
	if !ok {
		return Status{}, errtypes.NotFound(string(id))
	}
	if s.State == StateSucceeded || s.State == StateCancelled {
		return s, nil
	}
	s.CancelRequested = true
	s.State = StateCancelling
	f.rec[id] = s
	return s, nil
}

func (f *fakeStatus) List(context.Context, ListFilter) ([]Status, error) { return nil, nil }

func (f *fakeStatus) Reserve(_ context.Context, s Status, key string) (Status, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, k := range f.keys {
		if k == key && f.rec[id].Owner == s.Owner {
			return f.rec[id], false, nil
		}
	}
	f.rec[s.RunID] = s
	f.keys[s.RunID] = key
	f.pending[s.RunID] = time.Now()
	return s, true, nil
}

func (f *fakeStatus) Release(_ context.Context, id RunID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.keys, id)
	return nil
}

func (f *fakeStatus) MarkCommitted(_ context.Context, id RunID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.rec[id]; ok && s.State == StateQueued {
		delete(f.pending, id)
	}
	return nil
}

// revoke is the shared revocation: failed record, key freed, marker kept.
func (f *fakeStatus) revokeLocked(id RunID, reason string) bool {
	s, ok := f.rec[id]
	if !ok || s.State != StateQueued {
		return false
	}
	if _, pending := f.pending[id]; !pending {
		return false
	}
	now := time.Now()
	s.State = StateFailed
	s.FinishedAt = &now
	s.LastError = reason
	f.rec[id] = s
	delete(f.keys, id)
	return true
}

func (f *fakeStatus) Revoke(_ context.Context, id RunID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokeLocked(id, reason)
	return nil
}

func (f *fakeStatus) RevokePending(_ context.Context, grace time.Duration) ([]RunID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cutoff := time.Now().Add(-grace)
	var out []RunID
	for id, at := range f.pending {
		if at.Before(cutoff) && f.revokeLocked(id, "pending") {
			out = append(out, id)
		}
	}
	return out, nil
}

func (f *fakeStatus) Adopt(_ context.Context, skeleton Status, attempt int) (AdoptDecision, Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if attempt <= 0 {
		attempt = 1
	}
	s, ok := f.rec[skeleton.RunID]
	if !ok {
		now := time.Now()
		s = Status{
			RunID:      skeleton.RunID,
			Job:        skeleton.Job,
			State:      StateRunning,
			Attempt:    attempt,
			EnqueuedAt: skeleton.EnqueuedAt,
			StartedAt:  &now,
			Owner:      skeleton.Owner,
		}
		f.rec[s.RunID] = s
		return AdoptExecute, s, nil
	}

	switch {
	case s.State == StateSucceeded || s.State == StateCancelled:
		return AdoptObsolete, s, nil
	case s.State == StateFailed:
		if _, pending := f.pending[s.RunID]; pending {
			return AdoptObsolete, s, nil
		}
	case s.Attempt > attempt:
		return AdoptObsolete, s, nil
	case s.Attempt == attempt && s.State == StateRunning:
		return AdoptObsolete, s, nil
	}

	resubmit := s.Result != nil
	s.Attempt = attempt
	if s.CancelRequested {
		s.State = StateCancelling
	} else {
		s.State = StateRunning
	}
	s.FinishedAt = nil
	s.LastError = ""
	delete(f.pending, s.RunID)
	if !resubmit {
		now := time.Now()
		s.StartedAt = &now
	}
	f.rec[s.RunID] = s
	if resubmit {
		return AdoptResubmit, s, nil
	}
	return AdoptExecute, s, nil
}

func (f *fakeStatus) StageResult(_ context.Context, id RunID, attempt int, result Params) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.rec[id]
	if !ok || s.Attempt != attempt || (s.State != StateRunning && s.State != StateCancelling) {
		return errtypes.NotFound("fenced")
	}
	s.Result = result
	f.rec[id] = s
	return nil
}

func (f *fakeStatus) MarkFailed(_ context.Context, id RunID, attempt int, lastError string, at time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.rec[id]
	if !ok || s.Attempt != attempt {
		return false, nil
	}
	if s.State == StateSucceeded || s.State == StateCancelled || s.State == StateCancelling {
		return false, nil
	}
	s.State = StateFailed
	s.FinishedAt = &at
	s.LastError = lastError
	f.rec[id] = s
	return true, nil
}

func (f *fakeStatus) CommitTerminal(_ context.Context, st Status) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, fail := f.commitFailFor[st.RunID]; fail && a == st.Attempt {
		return false, errors.New("injected terminal commit failure")
	}
	cur, ok := f.rec[st.RunID]
	if !ok || cur.Attempt != st.Attempt {
		return false, nil
	}
	if cur.State == StateSucceeded || cur.State == StateCancelled {
		return false, nil
	}
	// a recorded cancel intent blocks a competing success; the cancel commit
	// itself is allowed from running (broadcast-only cancel) or cancelling.
	if st.State == StateSucceeded && cur.State == StateCancelling {
		return false, nil
	}
	st.CancelRequested = cur.CancelRequested
	f.rec[st.RunID] = st
	return true, nil
}

func (f *fakeStatus) Close(context.Context) error { return nil }

// test helpers

func (f *fakeStatus) keyOf(id RunID) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.keys[id]
	return k, ok
}

func (f *fakeStatus) pendingOf(id RunID) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.pending[id]
	return at, ok
}

func (f *fakeStatus) putPending(id RunID, s Status, key string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rec[id] = s
	f.pending[id] = at
	if key != "" {
		f.keys[id] = key
	}
}

func (f *fakeStatus) revokeDirect(id RunID, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revokeLocked(id, reason)
}

// oneRunStore hands out exactly one run, then blocks Claim until shutdown. It
// records whether the run was Completed (acked) or Failed (retried).
type oneRunStore struct {
	run       Run
	claimed   atomic.Bool
	completed atomic.Bool
	failed    atomic.Bool
}

func (s *oneRunStore) Enqueue(_ context.Context, r Run) (RunID, error) { return r.ID, nil }

func (s *oneRunStore) Claim(ctx context.Context) (Run, error) {
	if s.claimed.CompareAndSwap(false, true) {
		return s.run, nil
	}
	<-ctx.Done()
	return Run{}, ctx.Err()
}

func (s *oneRunStore) Complete(context.Context, RunID) error { s.completed.Store(true); return nil }
func (s *oneRunStore) Fail(context.Context, RunID, time.Duration) error {
	s.failed.Store(true)
	return nil
}
func (s *oneRunStore) Heartbeat(context.Context, RunID) error { return nil }
func (s *oneRunStore) HeartbeatInterval() time.Duration       { return 20 * time.Millisecond }
func (s *oneRunStore) DueScheduled(context.Context, time.Time) ([]ScheduledRun, error) {
	return nil, nil
}
func (s *oneRunStore) RegisterScheduled(context.Context, string, Schedule, time.Time, bool) error {
	return nil
}
func (s *oneRunStore) MarkScheduledRunning(context.Context, string) error  { return nil }
func (s *oneRunStore) ClearScheduledRunning(context.Context, string) error { return nil }
func (s *oneRunStore) TryMarkScheduledRunning(context.Context, string) (bool, error) {
	return false, nil
}
func (s *oneRunStore) RequestCancelScheduled(context.Context, string) (bool, error) {
	return false, nil
}
func (s *oneRunStore) ScheduledCancelRequested(context.Context, string) (bool, error) {
	return false, nil
}
func (s *oneRunStore) Close(context.Context) error { return nil }

func TestCancelStopsRunningJob(t *testing.T) {
	resetRegistry()

	started := make(chan struct{})
	if err := RegisterOnDemand("test.cancelme", func(context.Context, map[string]any) (Job, error) {
		return jobFunc(func(ctx context.Context, _ Params) (Params, error) {
			close(started)
			<-ctx.Done() // cooperative: stop as soon as the run is cancelled
			return nil, ctx.Err()
		}), nil
	}); err != nil {
		t.Fatal(err)
	}

	status := newFakeStatus()
	store := &oneRunStore{run: Run{ID: "run-1", Job: "test.cancelme", Attempt: 1}}
	_ = status.Put(context.Background(), Status{RunID: "run-1", Job: "test.cancelme", State: StateQueued})

	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	if _, err := r.Cancel(context.Background(), "run-1"); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for {
		got, _ := status.Get(context.Background(), "run-1")
		if got.State == StateCancelled {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run did not reach cancelled, last state %q", got.State)
		case <-time.After(5 * time.Millisecond):
		}
	}

	if !store.completed.Load() {
		t.Error("a cancelled run must be acked via Complete")
	}
	if store.failed.Load() {
		t.Error("a cancelled run must not be failed/retried")
	}
}

// busStore is a oneRunStore that also implements ControlBus, delivering a
// published cancel straight to the subscribed handler (loopback), so a test can
// simulate a cancel issued on another process.
type busStore struct {
	oneRunStore
	mu      sync.Mutex
	handler func(CancelSignal)
}

func (s *busStore) PublishCancel(_ context.Context, sig CancelSignal) error {
	s.mu.Lock()
	h := s.handler
	s.mu.Unlock()
	if h != nil {
		h(sig)
	}
	return nil
}

func (s *busStore) SubscribeCancel(_ context.Context, handler func(CancelSignal)) error {
	s.mu.Lock()
	s.handler = handler
	s.mu.Unlock()
	return nil
}

func TestCancelBroadcastStopsRemoteRun(t *testing.T) {
	resetRegistry()

	started := make(chan struct{})
	if err := RegisterOnDemand("test.remote", func(context.Context, map[string]any) (Job, error) {
		return jobFunc(func(ctx context.Context, _ Params) (Params, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}), nil
	}); err != nil {
		t.Fatal(err)
	}

	status := newFakeStatus()
	store := &busStore{oneRunStore: oneRunStore{run: Run{ID: "run-1", Job: "test.remote", Attempt: 1}}}
	_ = status.Put(context.Background(), Status{RunID: "run-1", Job: "test.remote", State: StateQueued})

	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not start")
	}

	// Simulate a cancel issued on another process: it reaches us only via the
	// bus, not through this runner's Cancel.
	if err := store.PublishCancel(context.Background(), CancelSignal{RunID: "run-1"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.After(2 * time.Second)
	for {
		got, _ := status.Get(context.Background(), "run-1")
		if got.State == StateCancelled {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run did not reach cancelled, last state %q", got.State)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if !store.completed.Load() {
		t.Error("a broadcast-cancelled run must be acked via Complete")
	}
}

// triggerStore records what TriggerNow does: whether it took the single-flight
// gate and what it enqueued.
type triggerStore struct {
	stubStore
	mu       sync.Mutex
	acquire  bool
	enqueued []string
}

func (s *triggerStore) TryMarkScheduledRunning(context.Context, string) (bool, error) {
	return s.acquire, nil
}

func (s *triggerStore) Enqueue(_ context.Context, run Run) (RunID, error) {
	s.mu.Lock()
	s.enqueued = append(s.enqueued, run.Job)
	s.mu.Unlock()
	return "rid", nil
}

func TestTriggerNow(t *testing.T) {
	resetRegistry()
	if err := RegisterPeriodic(Periodic{
		Name: "test.cleanup", Schedule: "@every 1h", Scope: ScopeLeader,
		Run: func(context.Context) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	store := &triggerStore{acquire: true}
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: newFakeStatus()})
	if err != nil {
		t.Fatal(err)
	}

	if err := r.TriggerNow(context.Background(), "test.cleanup"); err != nil {
		t.Fatal(err)
	}
	if len(store.enqueued) != 1 || store.enqueued[0] != "test.cleanup" {
		t.Fatalf("expected one enqueue of test.cleanup, got %v", store.enqueued)
	}

	// With a run already in flight the gate is not acquired: trigger is rejected
	// and enqueues nothing.
	store.acquire = false
	if err := r.TriggerNow(context.Background(), "test.cleanup"); err == nil {
		t.Error("expected an error triggering a job already in flight")
	}
	if len(store.enqueued) != 1 {
		t.Errorf("a rejected trigger must not enqueue, got %v", store.enqueued)
	}

	// On-demand and unregistered jobs cannot be triggered.
	if err := r.TriggerNow(context.Background(), "nope"); err == nil {
		t.Error("expected an error triggering an unregistered job")
	}
}

// periodicStore delivers one leader-periodic run and tracks the schedule's
// in-flight and cancel state by job name, like the real KV-backed store.
type periodicStore struct {
	run       Run
	claimed   atomic.Bool
	completed atomic.Bool
	mu        sync.Mutex
	running   bool
	cancelReq bool
}

func (s *periodicStore) Enqueue(_ context.Context, r Run) (RunID, error) { return r.ID, nil }

func (s *periodicStore) Claim(ctx context.Context) (Run, error) {
	if s.claimed.CompareAndSwap(false, true) {
		s.mu.Lock()
		s.running = true
		s.mu.Unlock()
		return s.run, nil
	}
	<-ctx.Done()
	return Run{}, ctx.Err()
}

func (s *periodicStore) Complete(context.Context, RunID) error            { s.completed.Store(true); return nil }
func (s *periodicStore) Fail(context.Context, RunID, time.Duration) error { return nil }
func (s *periodicStore) Heartbeat(context.Context, RunID) error           { return nil }
func (s *periodicStore) HeartbeatInterval() time.Duration                 { return 20 * time.Millisecond }
func (s *periodicStore) DueScheduled(context.Context, time.Time) ([]ScheduledRun, error) {
	return nil, nil
}
func (s *periodicStore) RegisterScheduled(context.Context, string, Schedule, time.Time, bool) error {
	return nil
}
func (s *periodicStore) MarkScheduledRunning(context.Context, string) error { return nil }
func (s *periodicStore) TryMarkScheduledRunning(context.Context, string) (bool, error) {
	return false, nil
}
func (s *periodicStore) ClearScheduledRunning(context.Context, string) error {
	s.mu.Lock()
	s.running = false
	s.cancelReq = false
	s.mu.Unlock()
	return nil
}

func (s *periodicStore) RequestCancelScheduled(context.Context, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return false, nil
	}
	s.cancelReq = true
	return true, nil
}

func (s *periodicStore) ScheduledCancelRequested(context.Context, string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cancelReq, nil
}

func (s *periodicStore) inFlight() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

func (s *periodicStore) Close(context.Context) error { return nil }

func TestCancelPeriodicStopsInFlightRun(t *testing.T) {
	resetRegistry()

	started := make(chan struct{})
	if err := RegisterPeriodic(Periodic{
		Name: "test.periodic", Schedule: "@every 1h", Scope: ScopeLeader,
		Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done() // cooperative: stop when the run is cancelled
			return ctx.Err()
		},
	}); err != nil {
		t.Fatal(err)
	}

	store := &periodicStore{run: Run{ID: "run-1", Job: "test.periodic", Attempt: 1}}
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: newFakeStatus()})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic job did not start")
	}

	if err := r.CancelPeriodic(context.Background(), "test.periodic"); err != nil {
		t.Fatal(err)
	}

	// Wait until the run has fully wound down: the in-flight mark is cleared by
	// the last deferred step of execRun, so this also implies the run was acked.
	deadline := time.After(2 * time.Second)
	for store.inFlight() {
		select {
		case <-deadline:
			t.Fatal("cancelled periodic run did not wind down")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if !store.completed.Load() {
		t.Fatal("cancelled periodic run was not acked via Complete")
	}

	// With nothing in flight, a second cancel is an error, not a silent no-op.
	if err := r.CancelPeriodic(context.Background(), "test.periodic"); err == nil {
		t.Error("expected an error cancelling a job with no run in flight")
	}
}

// scriptedStore plays back a fixed list of deliveries. The second delivery is
// held back until gate2 is closed, so a test can force a redelivery to race
// with the first attempt on its own terms. It records enqueues, acks and naks.
type scriptedStore struct {
	stubStore
	mu         sync.Mutex
	deliveries []Run
	next       int
	gate2      chan struct{}
	enqErr     error
	enqueued   []Run
	completed  int
	failed     []RunID
}

func (s *scriptedStore) Enqueue(_ context.Context, r Run) (RunID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.enqErr != nil {
		return "", s.enqErr
	}
	s.enqueued = append(s.enqueued, r)
	return r.ID, nil
}

func (s *scriptedStore) Claim(ctx context.Context) (Run, error) {
	s.mu.Lock()
	idx := s.next
	if idx < len(s.deliveries) {
		s.next++
		run := s.deliveries[idx]
		gate := s.gate2
		s.mu.Unlock()
		if idx == 1 && gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return Run{}, ctx.Err()
			}
		}
		return run, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return Run{}, ctx.Err()
}

func (s *scriptedStore) Complete(context.Context, RunID) error {
	s.mu.Lock()
	s.completed++
	s.mu.Unlock()
	return nil
}

func (s *scriptedStore) Fail(_ context.Context, id RunID, _ time.Duration) error {
	s.mu.Lock()
	s.failed = append(s.failed, id)
	s.mu.Unlock()
	return nil
}

func (s *scriptedStore) HeartbeatInterval() time.Duration { return 0 }

func (s *scriptedStore) completions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed
}

// eventually polls fn until it returns true or the deadline passes.
func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func registerTestJob(t *testing.T, name string, fn func(context.Context, Params) (Params, error)) {
	t.Helper()
	if err := RegisterOnDemand(name, func(context.Context, map[string]any) (Job, error) {
		return jobFunc(fn), nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueuePreassignsStableIdentity(t *testing.T) {
	resetRegistry()
	registerTestJob(t, "test.identity", func(context.Context, Params) (Params, error) { return nil, nil })

	store := &scriptedStore{}
	status := newFakeStatus()
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}

	id, err := r.Enqueue(context.Background(), "test.identity", Params{"x": 1}, WithOwner("alice"))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("enqueue must return the stable run id")
	}
	if len(store.enqueued) != 1 || store.enqueued[0].ID != id {
		t.Fatalf("the queue message must carry the preassigned id, got %+v", store.enqueued)
	}
	msg := store.enqueued[0]
	if msg.Attempt != 1 || msg.EnqueuedAt.IsZero() || msg.Owner != "alice" {
		t.Fatalf("message identity not fixed before publish: %+v", msg)
	}
	// on success the run is immediately queryable.
	st, err := status.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("queued run must be queryable: %v", err)
	}
	if st.State != StateQueued || !st.EnqueuedAt.Equal(msg.EnqueuedAt) {
		t.Fatalf("unexpected queued status: %+v", st)
	}
}

func TestEnqueueUniqueCommitsPhase(t *testing.T) {
	resetRegistry()
	registerTestJob(t, "test.unique-ok", func(context.Context, Params) (Params, error) { return nil, nil })

	store := &scriptedStore{}
	status := newFakeStatus()
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}

	id, err := r.Enqueue(context.Background(), "test.unique-ok", nil, Unique("space:1"), WithOwner("alice"))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := status.pendingOf(id); ok {
		t.Error("the publish-pending marker must be cleared after a successful publish")
	}
	if k, ok := status.keyOf(id); !ok || k != "space:1" {
		t.Errorf("the dedup key stays held until the run finishes, got %q/%v", k, ok)
	}

	// a concurrent enqueue collapses onto the same run and publishes nothing.
	id2, err := r.Enqueue(context.Background(), "test.unique-ok", nil, Unique("space:1"), WithOwner("alice"))
	if err != nil {
		t.Fatal(err)
	}
	if id2 != id || len(store.enqueued) != 1 {
		t.Fatalf("conflict must collapse onto the holder, id=%v enqueues=%d", id2, len(store.enqueued))
	}
}

func TestEnqueueUniquePublishFailureRevokesPlaceholder(t *testing.T) {
	resetRegistry()
	registerTestJob(t, "test.unique-fail", func(context.Context, Params) (Params, error) { return nil, nil })

	store := &scriptedStore{enqErr: errors.New("nats unavailable")}
	status := newFakeStatus()
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}

	id, err := r.Enqueue(context.Background(), "test.unique-fail", nil, Unique("space:2"), WithOwner("alice"))
	if err == nil {
		t.Fatal("a failed publish must fail the enqueue")
	}
	if id != "" {
		t.Fatalf("failed enqueue must not return an id, got %q", id)
	}

	// the failed enqueue leaves exactly one, revoked record.
	var only RunID
	for rid := range snapshotRecs(status) {
		only = rid
	}
	if only == "" {
		t.Fatal("the revoked run must remain recorded as a failed run")
	}
	got, err := status.Get(context.Background(), only)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StateFailed {
		t.Fatalf("revoked placeholder must be a failed record, got %q", got.State)
	}
	if _, held := status.keyOf(only); held {
		t.Error("the dedup key must be freed, not stuck behind a dead placeholder")
	}
	if _, ok := status.pendingOf(only); !ok {
		t.Error("the marker is kept so a late message is recognised as obsolete")
	}
	// a message that nevertheless arrives is dropped at adoption, never run.
	if decision, _, _ := status.Adopt(context.Background(),
		Status{RunID: only, Job: "test.unique-fail", Attempt: 1, EnqueuedAt: time.Now()}, 1); decision != AdoptObsolete {
		t.Fatalf("late message for a revoked row must be obsolete, got %v", decision)
	}
	// the key is immediately reusable.
	now := time.Now()
	if _, reserved, err := status.Reserve(context.Background(),
		Status{RunID: "again", Job: "test.unique-fail", State: StateQueued, Attempt: 1, EnqueuedAt: now, Owner: "alice"},
		"space:2"); err != nil || !reserved {
		t.Fatalf("the key must be reusable: reserved=%v err=%v", reserved, err)
	}
}

// snapshotRecs returns the ids currently recorded (test-only introspection).
func snapshotRecs(f *fakeStatus) map[RunID]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[RunID]struct{}, len(f.rec))
	for id := range f.rec {
		out[id] = struct{}{}
	}
	return out
}

func TestClaimRebuildsMissingStatus(t *testing.T) {
	resetRegistry()
	var ran int32
	registerTestJob(t, "test.rebuild", func(context.Context, Params) (Params, error) {
		atomic.AddInt32(&ran, 1)
		return Params{"p": "v"}, nil
	})

	enqueued := time.Now().Add(-time.Minute).Truncate(time.Second)
	store := &scriptedStore{deliveries: []Run{{
		ID: "orphan-1", Job: "test.rebuild", Attempt: 1, EnqueuedAt: enqueued, Owner: "alice",
	}}}
	status := newFakeStatus() // deliberately no row: publish landed, status did not
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	eventually(t, 2*time.Second, func() bool {
		st, err := status.Get(context.Background(), "orphan-1")
		return err == nil && st.State == StateSucceeded
	})
	st, _ := status.Get(context.Background(), "orphan-1")
	if st.Owner != "alice" || st.Job != "test.rebuild" || !st.EnqueuedAt.Equal(enqueued) {
		t.Errorf("rebuilt row must keep the original identity: %+v", st)
	}
	if st.Result["p"] != "v" {
		t.Errorf("rebuilt run result = %v", st.Result)
	}
	if atomic.LoadInt32(&ran) != 1 || store.completions() != 1 {
		t.Fatalf("run executed once and acked once: ran=%d completed=%d", ran, store.completions())
	}
}

func TestRedeliveryResubmitsStagedResult(t *testing.T) {
	resetRegistry()
	var invocations int32
	registerTestJob(t, "test.resubmit", func(context.Context, Params) (Params, error) {
		atomic.AddInt32(&invocations, 1)
		return Params{"answer": "42"}, nil
	})

	gate2 := make(chan struct{})
	store := &scriptedStore{
		gate2: gate2,
		deliveries: []Run{
			{ID: "run-r", Job: "test.resubmit", Attempt: 1, EnqueuedAt: time.Now()},
			{ID: "run-r", Job: "test.resubmit", Attempt: 2, EnqueuedAt: time.Now()},
		},
	}
	status := newFakeStatus()
	status.commitFailFor["run-r"] = 1 // attempt 1's terminal save fails
	r, err := NewRunner(context.Background(), Options{Workers: 2, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	// attempt 1 staged the result but failed to commit the terminal state.
	eventually(t, 2*time.Second, func() bool {
		st, err := status.Get(context.Background(), "run-r")
		return err == nil && st.Result != nil && st.State == StateRunning
	})
	close(gate2) // redelivery

	eventually(t, 2*time.Second, func() bool {
		st, _ := status.Get(context.Background(), "run-r")
		return st.State == StateSucceeded
	})
	st, _ := status.Get(context.Background(), "run-r")
	if st.Attempt != 2 || st.Result["answer"] != "42" {
		t.Fatalf("redelivery must commit the staged result at its attempt: %+v", st)
	}
	if n := atomic.LoadInt32(&invocations); n != 1 {
		t.Fatalf("the job must not run again on result resubmission, invocations=%d", n)
	}
	if store.completions() != 1 || len(store.failed) != 0 {
		t.Fatalf("exactly one ack, no nak: completed=%d failed=%v", store.completions(), store.failed)
	}
}

func TestOldAttemptCannotFormTerminalState(t *testing.T) {
	resetRegistry()
	var invocations int32
	started1 := make(chan struct{})
	registerTestJob(t, "test.fence", func(_ context.Context, _ Params) (Params, error) {
		n := atomic.AddInt32(&invocations, 1)
		if n == 1 {
			close(started1)
			// keep working until the redelivery has taken over, then return
			// late as the old owner.
			for {
				st, err := statusFence.Get(context.Background(), "run-f")
				if err == nil && st.Attempt >= 2 {
					return Params{"v": "old"}, nil
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
		return Params{"v": "new"}, nil
	})

	gate2 := make(chan struct{})
	store := &scriptedStore{
		gate2: gate2,
		deliveries: []Run{
			{ID: "run-f", Job: "test.fence", Attempt: 1, EnqueuedAt: time.Now()},
			{ID: "run-f", Job: "test.fence", Attempt: 2, EnqueuedAt: time.Now()},
		},
	}
	statusFence = newFakeStatus()
	r, err := NewRunner(context.Background(), Options{Workers: 2, Store: store, Status: statusFence})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	select {
	case <-started1:
	case <-time.After(2 * time.Second):
		t.Fatal("attempt 1 did not start")
	}
	eventually(t, 2*time.Second, func() bool {
		st, err := statusFence.Get(context.Background(), "run-f")
		return err == nil && st.Attempt == 1 && st.State == StateRunning
	})
	close(gate2) // heartbeat-timeout redelivery races attempt 1's result commit

	eventually(t, 2*time.Second, func() bool {
		st, _ := statusFence.Get(context.Background(), "run-f")
		return st.State == StateSucceeded
	})
	st, _ := statusFence.Get(context.Background(), "run-f")
	if st.Attempt != 2 || st.Result["v"] != "new" {
		t.Fatalf("only the current attempt forms the terminal state: %+v", st)
	}
	if store.completions() != 1 {
		t.Fatalf("the old owner must not ack; only attempt 2 completes: %d", store.completions())
	}
}

var statusFence *fakeStatus

func TestCancelQueuedRunBeforeFirstClaim(t *testing.T) {
	resetRegistry()
	ran := make(chan struct{}, 1)
	registerTestJob(t, "test.cancelqueued", func(context.Context, Params) (Params, error) {
		ran <- struct{}{}
		return nil, nil
	})

	status := newFakeStatus()
	now := time.Now()
	// cancel landed while the run was queued.
	if err := status.Put(context.Background(), Status{
		RunID: "run-q", Job: "test.cancelqueued", State: StateCancelling, Attempt: 1,
		EnqueuedAt: now, CancelRequested: true,
	}); err != nil {
		t.Fatal(err)
	}
	store := &scriptedStore{deliveries: []Run{{ID: "run-q", Job: "test.cancelqueued", Attempt: 1, EnqueuedAt: now}}}
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	eventually(t, 2*time.Second, func() bool {
		st, _ := status.Get(context.Background(), "run-q")
		return st.State == StateCancelled
	})
	if store.completions() != 1 {
		t.Fatal("a cancel before claim must be acked exactly once")
	}
	select {
	case <-ran:
		t.Fatal("the job must not execute when cancelled before its first claim")
	default:
	}
}

func TestObsoleteDeliveryForRevokedPlaceholder(t *testing.T) {
	resetRegistry()
	ran := make(chan struct{}, 1)
	registerTestJob(t, "test.obsolete", func(context.Context, Params) (Params, error) {
		ran <- struct{}{}
		return nil, nil
	})

	status := newFakeStatus()
	now := time.Now()
	status.putPending("run-o",
		Status{RunID: "run-o", Job: "test.obsolete", State: StateQueued, Attempt: 1, EnqueuedAt: now},
		"key-o", now)
	status.revokeDirect("run-o", "publication abandoned")

	store := &scriptedStore{deliveries: []Run{{ID: "run-o", Job: "test.obsolete", Attempt: 1, EnqueuedAt: now}}}
	r, err := NewRunner(context.Background(), Options{Workers: 1, Store: store, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	r.Start()
	defer r.Stop(context.Background())

	eventually(t, 2*time.Second, func() bool { return store.completions() == 1 })
	st, _ := status.Get(context.Background(), "run-o")
	if st.State != StateFailed {
		t.Errorf("revoked run must not be resurrected, got %q", st.State)
	}
	select {
	case <-ran:
		t.Fatal("an obsolete delivery must never execute the job")
	default:
	}
}

func TestRecoveryRevokesStalePendingReservations(t *testing.T) {
	status := newFakeStatus()
	old := time.Now().Add(-pendingPublishGrace - time.Minute)
	fresh := time.Now()
	status.putPending("stale",
		Status{RunID: "stale", Job: "j", State: StateQueued, Attempt: 1, EnqueuedAt: old},
		"k-stale", old)
	status.putPending("fresh",
		Status{RunID: "fresh", Job: "j", State: StateQueued, Attempt: 1, EnqueuedAt: fresh},
		"k-fresh", fresh)

	r, err := NewRunner(context.Background(), Options{Store: stubStore{}, Status: status})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.runRecovery(ctx)

	eventually(t, 2*time.Second, func() bool {
		st, err := status.Get(context.Background(), "stale")
		return err == nil && st.State == StateFailed
	})
	if _, held := status.keyOf("stale"); held {
		t.Error("stale placeholder must free its key")
	}
	if _, ok := status.pendingOf("stale"); !ok {
		t.Error("marker is kept so a late message is acked as obsolete")
	}
	st, _ := status.Get(context.Background(), "fresh")
	if st.State != StateQueued {
		t.Error("a fresh, in-flight reservation must not be revoked")
	}
	now := time.Now()
	if _, reserved, err := status.Reserve(context.Background(),
		Status{RunID: "next", Job: "j", State: StateQueued, Attempt: 1, EnqueuedAt: now}, "k-stale"); err != nil || !reserved {
		t.Fatalf("freed key must be reusable: reserved=%v err=%v", reserved, err)
	}
}
