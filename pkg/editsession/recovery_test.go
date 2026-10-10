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
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
)

// scriptedApp wraps the MemoryAppProvider to inject a timeout on takeover and
// script the QuerySaved reconciliation.
type scriptedApp struct {
	inner *MemoryAppProvider

	takeoverErr error
	// savedGen: when >= 0 QuerySaved reports saved at this generation; -1
	// means unsaved.
	savedGen int64
}

func (s *scriptedApp) TakeOver(ctx context.Context, sid string, r ResourceID, ref *provider.Reference, gen int64) error {
	if s.takeoverErr != nil {
		return s.takeoverErr
	}
	return s.inner.TakeOver(ctx, sid, r, ref, gen)
}

func (s *scriptedApp) QuerySaved(ctx context.Context, sid string, r ResourceID) (bool, int64, error) {
	// the session must still be known to the inner app even when its save
	// state is scripted.
	if _, _, err := s.inner.QuerySaved(ctx, sid, r); err != nil {
		return false, 0, err
	}
	if s.savedGen < 0 {
		return false, 0, nil
	}
	return true, s.savedGen, nil
}

func (s *scriptedApp) ConfirmReference(ctx context.Context, r ResourceID, ref *provider.Reference, gen int64) error {
	return s.inner.ConfirmReference(ctx, r, ref, gen)
}

func replaceApp(g *rig, app editsession.AppProvider) {
	g.coord = editsession.New(g.store, g.fs, g.locks, app, g.ends, editsession.Config{
		DrainPoll:    time.Millisecond,
		DrainTimeout: 2 * time.Second,
	})
}

func TestAppCallbackTimeoutReconciledSaved(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	app := &scriptedApp{inner: g.app, takeoverErr: editsession.ErrAppCallbackTimeout, savedGen: 2}
	replaceApp(g, app)

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("move should reconcile via QuerySaved: %v", err)
	}
	if got.State != GenConfirmed {
		t.Fatalf("expected confirmed after reconciliation, got %s", got.State)
	}
	// exactly one session, bound to the new generation: no duplicates.
	sessions, _ := g.store.ListSessions(ctx, g.r, 0, false)
	if len(sessions) != 1 || sessions[0].Generation != 2 {
		t.Fatalf("expected a single relocated session, got %+v", sessions)
	}
}

func TestAppCallbackTimeoutUnverifiedKeepsMoving(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	app := &scriptedApp{inner: g.app, takeoverErr: editsession.ErrAppCallbackTimeout, savedGen: -1}
	replaceApp(g, app)

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err == nil {
		t.Fatal("expected unverified takeover to surface an error")
	}
	if got.State != GenMoving {
		t.Fatalf("generation must remain moving and be resumable, got %s", got.State)
	}

	// the failure is not masked by a new move: after the app becomes healthy a
	// repeated Move resumes and confirms the SAME generation.
	app.takeoverErr = nil
	got, err = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("resumed move: %v", err)
	}
	if got.Sequence != 2 || got.State != GenConfirmed {
		t.Fatalf("expected the same generation confirmed, got seq=%d state=%s", got.Sequence, got.State)
	}
}

func TestLockRefreshFailurePreservesOldLocation(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	// simulate that the lock travels with the inode: lock appears at the new
	// reference, and its refresh fails once.
	g.locks.SetLock(ctx, g.newRef(), &provider.Lock{LockId: "lock-1"})
	var calls int
	g.locks.RefreshLockHook = func(_ *provider.Reference) error {
		calls++
		if calls == 1 {
			return errors.New("lock refresh failed")
		}
		return nil
	}

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err == nil {
		t.Fatal("expected lock failure surfaced")
	}
	if got.State != GenAborted {
		t.Fatalf("new generation must be aborted, got %s", got.State)
	}

	// entry returned to the old location.
	if _, err := g.fs.Stat(ctx, g.oldRef()); err != nil {
		t.Fatalf("old location must exist again: %v", err)
	}
	if info, err := g.fs.Stat(ctx, g.newRef()); err == nil {
		t.Fatalf("new location must be gone, got %+v", info)
	}

	// the old lock is still held by the original holder; the stale new lock
	// was cleaned during abort.
	if !g.locks.HasLock(g.oldRef()) {
		t.Fatal("old location lock must survive")
	}
	if g.locks.HasLock(g.newRef()) {
		t.Fatal("new reference lock must be released on abort")
	}

	// service restored at the old location: opens and saves work.
	if err := g.coord.CheckEditOpen(ctx, g.r); err != nil {
		t.Fatalf("edit opens must be restored: %v", err)
	}
	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-restored", Resource: g.r}); err != nil {
		t.Fatalf("saves at the old location must work: %v", err)
	}
	if err := g.coord.CommitAllowed(ctx, "up-restored"); err != nil {
		t.Fatalf("restored save must be committable: %v", err)
	}
}

// crashPhase identifies at which point a crash is simulated.
type crashPhase string

const (
	crashFreezingUnmovedMarked   crashPhase = "freezing-unmoved-marked"
	crashFreezingUnmovedUnmarked crashPhase = "freezing-unmoved-unmarked"
	crashFreezingMoved           crashPhase = "freezing-moved"
	crashMovingNoConfirmations   crashPhase = "moving-none"
	crashMovingPartial           crashPhase = "moving-partial"
)

func seedCrashedGeneration(t *testing.T, g *rig, phase crashPhase) {
	t.Helper()
	ctx := context.Background()

	now := time.Now()
	gen := &Generation{
		Sequence: 2, Resource: g.r,
		OldPath: oldPath, NewPath: newPath,
		State: GenFreezing, Confirmations: map[editsession.Party]bool{},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := g.store.CreateGeneration(ctx, gen); err != nil {
		t.Fatalf("create crashed generation: %v", err)
	}

	entryMoved := phase == crashFreezingMoved || phase == crashMovingNoConfirmations || phase == crashMovingPartial
	markerSet := phase != crashFreezingUnmovedUnmarked
	moving := phase == crashMovingNoConfirmations || phase == crashMovingPartial

	if entryMoved {
		if err := g.fs.Move(ctx, g.oldRef(), g.newRef()); err != nil {
			t.Fatalf("move entry to new: %v", err)
		}
	}
	if markerSet {
		if err := g.store.MarkSuperseded(ctx, g.r, 1, 2); err != nil {
			t.Fatalf("mark superseded: %v", err)
		}
	}
	if moving {
		if _, err := g.store.TransitionGeneration(ctx, g.r, 2,
			[]editsession.GenState{editsession.GenFreezing}, editsession.GenMoving); err != nil {
			t.Fatalf("transition to moving: %v", err)
		}
	}
	if phase == crashMovingPartial {
		if err := g.store.RecordConfirmation(ctx, g.r, 2, editsession.PartyLock); err != nil {
			t.Fatalf("record partial confirmation: %v", err)
		}
	}
}

func TestRestartRecoversEveryPhase(t *testing.T) {
	for _, phase := range []crashPhase{
		crashFreezingUnmovedMarked,
		crashFreezingUnmovedUnmarked,
		crashFreezingMoved,
		crashMovingNoConfirmations,
		crashMovingPartial,
	} {
		t.Run(string(phase), func(t *testing.T) {
			g := newRig(t)
			ctx := context.Background()
			g.seedSession(t, "s1")
			seedCrashedGeneration(t, g, phase)

			// simulate a process restart: a brand new coordinator over the
			// same persistent store resumes the generation from its phase.
			recovered := editsession.New(g.store, g.fs, g.locks, g.app, g.ends, editsession.Config{
				DrainPoll:    time.Millisecond,
				DrainTimeout: 2 * time.Second,
			})
			if err := recovered.Recover(ctx); err != nil {
				t.Fatalf("recover: %v", err)
			}

			got, err := g.store.GetGeneration(ctx, g.r, 2)
			if err != nil {
				t.Fatalf("get generation: %v", err)
			}
			if got.State != GenConfirmed {
				t.Fatalf("expected confirmed after recovery, got %s", got.State)
			}

			// recovery reuses the same generation and sessions: zero new
			// sessions, the one session bound to the new generation.
			gens, _ := g.store.ListGenerations(ctx, g.r)
			if len(gens) != 2 {
				t.Fatalf("expected the same 2 generations, got %d", len(gens))
			}
			sessions, _ := g.store.ListSessions(ctx, g.r, 0, false)
			if len(sessions) != 1 || sessions[0].Generation != 2 {
				t.Fatalf("expected a single relocated session, got %+v", sessions)
			}
		})
	}
}

func TestDuplicateMoveJoinsGeneration(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")
	// a move is already in freezing state for the resource.
	seedCrashedGeneration(t, g, crashFreezingUnmovedMarked)

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("duplicate move should join the generation: %v", err)
	}
	if got.Sequence != 2 || got.State != GenConfirmed {
		t.Fatalf("expected the in-progress generation confirmed, got seq=%d state=%s", got.Sequence, got.State)
	}
	gens, _ := g.store.ListGenerations(ctx, g.r)
	if len(gens) != 2 {
		t.Fatalf("no second generation must be created, got %d", len(gens))
	}
}
