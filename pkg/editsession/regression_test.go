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

package editsession_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sp "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
)

// TestConcurrentColdMoves covers P-1: two moves arriving before any
// generation exists must converge on a single new generation instead of one
// surfacing an internal conflict. A start barrier makes the two goroutines
// race for real instead of one possibly running after the other finished.
func TestConcurrentColdMoves(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]*editsession.Generation, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			t.Fatalf("move %d returned error: %v", i, errs[i])
		}
		if results[i].Sequence != 2 || results[i].State != editsession.GenConfirmed {
			t.Fatalf("move %d did not converge on confirmed G2: %+v", i, results[i])
		}
	}
	gens, _ := g.store.ListGenerations(ctx, g.r)
	if len(gens) != 2 {
		t.Fatalf("expected exactly 2 generations, got %d", len(gens))
	}
	if moves := g.fs.Moves(); len(moves) != 1 {
		t.Fatalf("storage Move must run once, got %d", len(moves))
	}
}

// scriptedInProgressStore returns no in-progress generation on the first
// call and delegates afterwards, so the F-1 window can be driven
// deterministically: Move passes the initial check, finds no open sessions
// on the predecessor, and must re-check and join the generation that
// appeared meanwhile instead of returning ErrNoSessions.
type scriptedInProgressStore struct {
	editsession.Store
	mu    sync.Mutex
	calls int
}

func (s *scriptedInProgressStore) GetInProgressGeneration(ctx context.Context, r editsession.ResourceID) (*editsession.Generation, error) {
	s.mu.Lock()
	s.calls++
	first := s.calls == 1
	s.mu.Unlock()
	if first {
		return nil, nil
	}
	return s.Store.GetInProgressGeneration(ctx, r)
}

func TestMoveJoinsGenerationAppearingBetweenChecks(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	// A concurrent move has started after our initial in-progress check:
	// G1 is the former serving generation, G2 is moving and its session
	// has already been relocated to G2 (active). Move must observe the
	// unfinished latest generation and join it, never open a competing G3.
	now := time.Now()
	g.store.CreateGeneration(ctx, &editsession.Generation{Sequence: 1, Resource: g.r, State: editsession.GenActive})
	g.store.CreateGeneration(ctx, &editsession.Generation{Sequence: 2, Resource: g.r, State: editsession.GenMoving,
		Confirmations: map[editsession.Party]bool{}, CreatedAt: now, UpdatedAt: now})
	g.store.CreateSession(ctx, &editsession.Session{
		ID: "s1", Resource: g.r, Generation: 2, State: editsession.SessionActive,
	})

	wrapped := &scriptedInProgressStore{Store: g.store}
	coord := editsession.New(wrapped, g.fs, g.locks, g.app, g.ends,
		editsession.Config{DrainPoll: time.Millisecond})

	got, err := coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("expected Move to join the appeared generation, got error: %v", err)
	}
	if got.Sequence != 2 || got.State != editsession.GenConfirmed {
		t.Fatalf("expected confirmed G2 via join, got seq=%d state=%s", got.Sequence, got.State)
	}
}

// TestRecoverDoesNotClearLaterGenerationMarker covers the N-1 hazard: an
// aborted generation retained from an earlier move must not let recovery
// clear the marker a later, confirmed generation set.
func TestRecoverDoesNotClearLaterGenerationMarker(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	now := time.Now()
	// G1 active; G2 an old aborted generation; G3 currently confirmed.
	g.store.CreateGeneration(ctx, &editsession.Generation{Sequence: 1, Resource: g.r, State: editsession.GenActive})
	g.store.CreateGeneration(ctx, &editsession.Generation{Sequence: 2, Resource: g.r, State: editsession.GenAborted,
		Confirmations: map[editsession.Party]bool{}, CreatedAt: now, UpdatedAt: now})
	g3 := &editsession.Generation{Sequence: 3, Resource: g.r, State: editsession.GenConfirmed,
		Confirmations: map[editsession.Party]bool{
			editsession.PartySessions: true, editsession.PartyAppProvider: true,
			editsession.PartyLock:     true,
			editsession.PartyDownload: true, editsession.PartyUpload: true,
		},
		CreatedAt: now, UpdatedAt: now}
	g.store.CreateGeneration(ctx, g3)
	g.store.MarkSuperseded(ctx, g.r, 1, 3)

	if err := g.coord.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	after, err := g.store.GetGeneration(ctx, g.r, 1)
	if err != nil {
		t.Fatalf("get g1: %v", err)
	}
	if after.SupersededBy != 3 {
		t.Fatalf("G3 marker must survive recovery, SupersededBy=%d", after.SupersededBy)
	}
}

// TestMoveAfterLockAbort covers P-2: after a lock failure aborts the move,
// sessions are rebound to the predecessor and a later move works normally.
func TestMoveAfterLockAbort(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	// lock travels with the inode; its refresh fails once, aborting G2.
	g.locks.SetLock(ctx, g.newRef(), &sp.Lock{LockId: "lock-1"})
	var calls int
	g.locks.RefreshLockHook = func(_ *sp.Reference) error {
		calls++
		if calls == 1 {
			return errors.New("lock refresh failed")
		}
		return nil
	}

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err == nil {
		t.Fatal("expected the first move to surface the lock failure")
	}
	if got.State != editsession.GenAborted {
		t.Fatalf("expected aborted, got %s", got.State)
	}

	// session must have been rebound to G1 (not stuck on the aborted G2).
	sessions, _ := g.store.ListSessions(ctx, g.r, 0, false)
	if len(sessions) != 1 || sessions[0].Generation != 1 {
		t.Fatalf("session not rebound to predecessor: %+v", sessions)
	}

	// the transient is gone: a subsequent move succeeds end to end.
	got, err = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("move after abort failed: %v", err)
	}
	// the aborted G2 is retained for audit, so the successful retry is G3.
	if got.Sequence != 3 || got.State != editsession.GenConfirmed {
		t.Fatalf("expected a fresh confirmed G3, got seq=%d state=%s", got.Sequence, got.State)
	}
}

// TestRecoverClearsMarkerAfterInterruptedAbort covers the last N-2 crash
// window: all sessions were rebound before the crash, but the predecessor
// marker was never cleared. Recovery must finish that final write, otherwise
// the serving generation would later be deleted and generation numbers
// reused.
func TestRecoverClearsMarkerAfterInterruptedAbort(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	g1 := &editsession.Generation{Sequence: 1, Resource: g.r, State: editsession.GenActive}
	g2 := &editsession.Generation{Sequence: 2, Resource: g.r, State: editsession.GenAborted,
		Confirmations: map[editsession.Party]bool{}}
	g.store.CreateGeneration(ctx, g1)
	g.store.CreateGeneration(ctx, g2)
	// rollback state: marker present although the session was already rebound.
	g.store.MarkSuperseded(ctx, g.r, 1, 2)
	g.store.CreateSession(ctx, &editsession.Session{ID: "s1", Resource: g.r, Generation: 1})

	if err := g.coord.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	after, err := g.store.GetGeneration(ctx, g.r, 1)
	if err != nil {
		t.Fatalf("get g1: %v", err)
	}
	if after.SupersededBy != 0 {
		t.Fatalf("marker must be cleared, SupersededBy=%d", after.SupersededBy)
	}
	// idempotent: a second recovery changes nothing.
	if err := g.coord.Recover(ctx); err != nil {
		t.Fatalf("second recover: %v", err)
	}
}

// TestRecordCallbackDistinguishesKind covers N-3: a saved and a closed
// callback with the same sequence are distinct and both recorded.
func TestRecordCallbackDistinguishesKind(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	saved, err := g.store.RecordCallback(ctx, g.r, "s1", 1, editsession.CallbackSaved)
	if err != nil || !saved {
		t.Fatalf("saved callback must be new: %v %v", saved, err)
	}
	closed, err := g.store.RecordCallback(ctx, g.r, "s1", 1, editsession.CallbackClosed)
	if err != nil || !closed {
		t.Fatalf("closed callback with same seq must be new: %v %v", closed, err)
	}
	repeated, err := g.store.RecordCallback(ctx, g.r, "s1", 1, editsession.CallbackClosed)
	if err != nil || repeated {
		t.Fatalf("repeated callback must not be new: %v %v", repeated, err)
	}
}

// TestOpenSessionWhileFrozen covers P-3's externally visible contract: with
// a move in flight a new read-write open is told to retry, never silently
// registered.
func TestOpenSessionWhileFrozen(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()

	// base generation exists; a new move is freezing.
	base := &editsession.Generation{Sequence: 1, Resource: g.r, State: editsession.GenActive}
	g.store.CreateGeneration(ctx, base)
	g2 := &editsession.Generation{Sequence: 2, Resource: g.r, State: editsession.GenFreezing,
		Confirmations: map[editsession.Party]bool{}}
	g.store.CreateGeneration(ctx, g2)

	sess := &editsession.Session{ID: "s2", Resource: g.r}
	err := g.coord.OpenSession(ctx, sess)
	if !errors.Is(err, editsession.ErrFrozen) {
		t.Fatalf("expected frozen rejection, got %v", err)
	}
	// no session row may be written while frozen.
	if _, gErr := g.store.GetSession(ctx, "s2"); gErr == nil {
		t.Fatal("no session should exist after frozen rejection")
	}
}

// TestUnlockReleasesSessions covers P-4: releasing the write lock closes
// the editing sessions bound to it and triggers the old-generation cleanup.
func TestUnlockReleasesSessions(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	if _, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef()); err != nil {
		t.Fatalf("move: %v", err)
	}
	// session is open at G2; G1 is still present before unlock.
	if n, _ := g.store.CountOpenSessions(ctx, g.r, 0); n != 1 {
		t.Fatalf("pre-unlock open sessions = %d, want 1", n)
	}

	err := g.coord.Unlock(ctx, g.r, g.newRef(), &sp.Lock{LockId: "lock-1"})
	if err != nil {
		t.Fatalf("unlock: %v", err)
	}

	// session closed...
	if n, _ := g.store.CountOpenSessions(ctx, g.r, 0); n != 0 {
		t.Fatalf("post-unlock open sessions = %d, want 0", n)
	}
	// ...and the old generation was cleaned, only confirmed G2 remains.
	gens, _ := g.store.ListGenerations(ctx, g.r)
	if len(gens) != 1 || gens[0].Sequence != 2 || gens[0].State != editsession.GenConfirmed {
		var got []int64
		for _, x := range gens {
			got = append(got, x.Sequence)
		}
		t.Fatalf("old generation not cleaned, remaining: %v", got)
	}
}
