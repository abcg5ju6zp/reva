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

package sql

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cs3org/reva/v3/cmd/revad/pkg/config"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/rjobs"
	"github.com/cs3org/reva/v3/pkg/rjobs/store/sql/model"
)

func newTestStore(t *testing.T) rjobs.StatusStore {
	t.Helper()
	dbFile := filepath.Join(t.TempDir(), "jobs.db")
	s, err := New(context.Background(), config.Database{Engine: "sqlite", DBName: dbFile})
	if err != nil {
		t.Fatalf("creating store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func TestPutAndGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	if err := s.Put(ctx, rjobs.Status{
		RunID:      "run-1",
		Job:        "example.pingpong",
		State:      rjobs.StateQueued,
		Attempt:    1,
		EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	// upsert: move to succeeded with a result.
	started := now.Add(time.Second)
	finished := now.Add(2 * time.Second)
	if err := s.Put(ctx, rjobs.Status{
		RunID:      "run-1",
		Job:        "example.pingpong",
		State:      rjobs.StateSucceeded,
		Attempt:    1,
		EnqueuedAt: now,
		StartedAt:  &started,
		FinishedAt: &finished,
		Result:     rjobs.Params{"pong": "pong: hi"},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != rjobs.StateSucceeded {
		t.Errorf("state = %q, want succeeded", got.State)
	}
	if got.Result["pong"] != "pong: hi" {
		t.Errorf("result = %v, want pong: hi", got.Result["pong"])
	}
	if got.StartedAt == nil || got.FinishedAt == nil {
		t.Error("started/finished timestamps should be set")
	}
}

func TestGetNotFound(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(context.Background(), "missing")
	if _, ok := err.(errtypes.NotFound); !ok {
		t.Errorf("expected NotFound, got %T: %v", err, err)
	}
}

func TestListByOwner(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	alice := "alice"
	bob := "bob"

	put := func(id, owner, job string, state rjobs.State, enqueued time.Time) {
		t.Helper()
		if err := s.Put(ctx, rjobs.Status{
			RunID:      rjobs.RunID(id),
			Job:        job,
			State:      state,
			Attempt:    1,
			EnqueuedAt: enqueued,
			Owner:      owner,
		}); err != nil {
			t.Fatal(err)
		}
	}

	put("a1", alice, "export", rjobs.StateQueued, now)
	put("a2", alice, "report", rjobs.StateSucceeded, now.Add(time.Second))
	put("b1", bob, "export", rjobs.StateQueued, now)
	put("internal", "", "cleanup", rjobs.StateRunning, now)

	// all of alice's runs, most recently enqueued first.
	got, err := s.List(ctx, rjobs.ListFilter{Owner: alice})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("alice runs = %d, want 2", len(got))
	}
	if got[0].RunID != "a2" || got[1].RunID != "a1" {
		t.Errorf("order = %q,%q, want a2,a1", got[0].RunID, got[1].RunID)
	}
	if got[0].Owner != "alice" {
		t.Errorf("owner not round-tripped: %q", got[0].Owner)
	}

	// narrowing by state keeps only the matching run.
	got, err = s.List(ctx, rjobs.ListFilter{Owner: alice, States: []rjobs.State{rjobs.StateQueued}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RunID != "a1" {
		t.Fatalf("queued alice runs = %v, want [a1]", got)
	}

	// another user's listing is scoped to their own runs only.
	got, err = s.List(ctx, rjobs.ListFilter{Owner: bob})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RunID != "b1" {
		t.Fatalf("bob runs = %v, want [b1]", got)
	}

	// the internal-only filter returns just the no-owner runs.
	got, err = s.List(ctx, rjobs.ListFilter{Internal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].RunID != "internal" {
		t.Fatalf("internal runs = %v, want [internal]", got)
	}
}

func TestReserve(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	alice := "alice"
	bob := "bob"
	queued := func(id, owner string) rjobs.Status {
		return rjobs.Status{RunID: rjobs.RunID(id), Job: "export", State: rjobs.StateQueued, Attempt: 1, EnqueuedAt: now, Owner: owner}
	}

	// the first reservation of a key wins.
	if _, reserved, err := s.Reserve(ctx, queued("r1", alice), "export:1"); err != nil || !reserved {
		t.Fatalf("first reservation should win: reserved=%v err=%v", reserved, err)
	}

	// a second one for the same (owner, key) collapses onto the holder.
	existing, reserved, err := s.Reserve(ctx, queued("r2", alice), "export:1")
	if err != nil {
		t.Fatal(err)
	}
	if reserved {
		t.Fatal("second reservation must not win while the key is held")
	}
	if existing.RunID != "r1" {
		t.Errorf("holder = %q, want r1", existing.RunID)
	}

	// a different user holding the same key is independent.
	if _, reserved, err := s.Reserve(ctx, queued("r3", bob), "export:1"); err != nil || !reserved {
		t.Fatalf("a different owner should reserve the same key: reserved=%v err=%v", reserved, err)
	}

	// once released, the key is free for a new run.
	if err := s.Release(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, reserved, err := s.Reserve(ctx, queued("r4", alice), "export:1"); err != nil || !reserved {
		t.Fatalf("a released key should be reservable again: reserved=%v err=%v", reserved, err)
	}
}

func queuedStatus(id, owner string, at time.Time) rjobs.Status {
	return rjobs.Status{RunID: rjobs.RunID(id), Job: "export", State: rjobs.StateQueued, Attempt: 1, EnqueuedAt: at, Owner: owner}
}

func TestReserveStartsPublishPendingPhase(t *testing.T) {
	s := newTestStore(t).(*store)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if _, reserved, err := s.Reserve(ctx, queuedStatus("r1", "alice", now), "export:1"); err != nil || !reserved {
		t.Fatalf("reserve failed: reserved=%v err=%v", reserved, err)
	}
	var row model.Run
	if err := s.db.First(&row, "run_id = ?", "r1").Error; err != nil {
		t.Fatal(err)
	}
	if row.PublishPendingAt == nil {
		t.Fatal("Reserve must enter the publish-pending phase")
	}

	if err := s.MarkCommitted(ctx, "r1"); err != nil {
		t.Fatal(err)
	}
	// fresh struct: gorm leaves an already-pointing pointer untouched when the
	// column is NULL, so reusing row would mask the update.
	var after model.Run
	if err := s.db.First(&after, "run_id = ?", "r1").Error; err != nil {
		t.Fatal(err)
	}
	if after.PublishPendingAt != nil {
		t.Fatal("MarkCommitted must clear the pending marker")
	}
}

func TestRevokeKeepsMarkerAndFreesKey(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if _, reserved, err := s.Reserve(ctx, queuedStatus("r1", "alice", now), "export:9"); err != nil || !reserved {
		t.Fatalf("reserve failed: reserved=%v err=%v", reserved, err)
	}
	if err := s.Revoke(ctx, "r1", "publish failed"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if got.State != rjobs.StateFailed {
		t.Fatalf("revoked row state = %q, want failed", got.State)
	}
	// key reusable immediately.
	if _, reserved, err := s.Reserve(ctx, queuedStatus("r2", "alice", now), "export:9"); err != nil || !reserved {
		t.Fatalf("the key must be freed by revoke: reserved=%v err=%v", reserved, err)
	}
	// but a late message for the revoked row is obsolete.
	if decision, _, err := s.Adopt(ctx,
		rjobs.Status{RunID: "r1", Job: "export", Attempt: 1, EnqueuedAt: now, Owner: "alice"}, 1); err != nil {
		t.Fatal(err)
	} else if decision != rjobs.AdoptObsolete {
		t.Fatalf("late message decision = %v, want obsolete", decision)
	}
}

func TestRevokePendingOnlyOldRows(t *testing.T) {
	s := newTestStore(t).(*store)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if _, _, err := s.Reserve(ctx, queuedStatus("fresh", "alice", now), "k-fresh"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Reserve(ctx, queuedStatus("stale", "bob", now), "k-stale"); err != nil {
		t.Fatal(err)
	}
	// simulate the stale enqueue dying minutes before publication.
	if err := s.db.Model(&model.Run{}).Where("run_id = ?", "stale").
		Update("publish_pending_at", now.Add(-10*time.Minute)).Error; err != nil {
		t.Fatal(err)
	}

	revoked, err := s.RevokePending(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(revoked) != 1 || revoked[0] != "stale" {
		t.Fatalf("revoked = %v, want [stale]", revoked)
	}
	stale, _ := s.Get(ctx, "stale")
	if stale.State != rjobs.StateFailed {
		t.Errorf("stale row state = %q, want failed", stale.State)
	}
	fresh, _ := s.Get(ctx, "fresh")
	if fresh.State != rjobs.StateQueued {
		t.Errorf("fresh row state = %q, want queued", fresh.State)
	}
	// running the sweep again must not touch committed/queued rows.
	again, err := s.RevokePending(ctx, time.Minute)
	if err != nil || len(again) != 0 {
		t.Fatalf("second sweep = %v, %v; want none", again, err)
	}
}

func TestAdoptRebuildsMissingStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	decision, got, err := s.Adopt(ctx,
		rjobs.Status{RunID: "orphan", Job: "export", Attempt: 1, EnqueuedAt: now, Owner: "alice"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if decision != rjobs.AdoptExecute {
		t.Fatalf("decision = %v, want execute", decision)
	}
	if got.State != rjobs.StateRunning || got.Attempt != 1 || got.Owner != "alice" || got.StartedAt == nil {
		t.Fatalf("rebuilt row wrong: %+v", got)
	}
	// the original run is queryable under its original identity now.
	again, err := s.Get(ctx, "orphan")
	if err != nil || again.Job != "export" || !again.EnqueuedAt.Equal(now) {
		t.Fatalf("rebuilt run not queryable: %+v %v", again, err)
	}
}

func TestAdoptFencesAttemptsAndDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, _, err := s.Reserve(ctx, queuedStatus("r1", "alice", now), "k"); err != nil {
		t.Fatal(err)
	}

	// first delivery takes ownership and confirms the publication.
	d1, st, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 1)
	if err != nil || d1 != rjobs.AdoptExecute {
		t.Fatalf("adopt 1: decision=%v err=%v", d1, err)
	}
	if st.State != rjobs.StateRunning {
		t.Fatalf("adopt 1 state = %q", st.State)
	}

	// a duplicate same-attempt delivery must not run the job a second time.
	if decision, _, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 1); err != nil || decision != rjobs.AdoptObsolete {
		t.Fatalf("duplicate delivery: decision=%v err=%v", decision, err)
	}

	// the redelivery (attempt 2) takes over.
	d2, st, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 2)
	if err != nil || d2 != rjobs.AdoptExecute {
		t.Fatalf("adopt 2: decision=%v err=%v", d2, err)
	}
	if st.Attempt != 2 || st.State != rjobs.StateRunning {
		t.Fatalf("adopt 2 row = %+v", st)
	}
	if st.FinishedAt != nil {
		t.Fatal("taking over must clear the previous attempt's finished_at")
	}

	// the old owner's late delivery now stands down.
	if decision, _, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 1); err != nil || decision != rjobs.AdoptObsolete {
		t.Fatalf("old delivery: decision=%v err=%v", decision, err)
	}
}

func TestAdoptObsoleteTerminalRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	for _, terminal := range []rjobs.State{rjobs.StateSucceeded, rjobs.StateCancelled} {
		id := rjobs.RunID("done-" + string(terminal))
		if err := s.Put(ctx, rjobs.Status{
			RunID: id, Job: "j", State: terminal, Attempt: 1, EnqueuedAt: now,
		}); err != nil {
			t.Fatal(err)
		}
		if decision, _, err := s.Adopt(ctx, rjobs.Status{RunID: id, Job: "j", Attempt: 2, EnqueuedAt: now}, 2); err != nil {
			t.Fatal(err)
		} else if decision != rjobs.AdoptObsolete {
			t.Fatalf("terminal %s: decision = %v, want obsolete", terminal, decision)
		}
	}
}

func TestAdoptResubmitsStagedResult(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if _, _, err := s.Reserve(ctx, queuedStatus("r1", "alice", now), "k"); err != nil {
		t.Fatal(err)
	}
	if _, st, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 1); err != nil {
		t.Fatal(err)
	} else if st.StartedAt == nil {
		t.Fatal("missing started_at")
	}
	if err := s.StageResult(ctx, "r1", 1, rjobs.Params{"answer": "42"}); err != nil {
		t.Fatal(err)
	}

	// redelivery: the existing result is adopted without re-running.
	before, _ := s.Get(ctx, "r1")
	decision, st, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 2)
	if err != nil {
		t.Fatal(err)
	}
	if decision != rjobs.AdoptResubmit {
		t.Fatalf("decision = %v, want resubmit", decision)
	}
	if st.Result["answer"] != "42" {
		t.Fatalf("staged result lost: %v", st.Result)
	}
	if st.StartedAt == nil || !st.StartedAt.Equal(*before.StartedAt) {
		t.Fatal("a resubmit must preserve the original start time")
	}
}

func TestStagedAndFailedWritesAreAttemptFenced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, _, err := s.Reserve(ctx, queuedStatus("r1", "alice", now), "k"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 1); err != nil {
		t.Fatal(err)
	}
	// attempt 2 takes over (e.g. heartbeat redelivery).
	if _, _, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 2); err != nil {
		t.Fatal(err)
	}
	// old owner's late writes lose the fence.
	if err := s.StageResult(ctx, "r1", 1, rjobs.Params{"late": true}); err == nil {
		t.Fatal("staging from the old attempt must be rejected")
	}
	if committed, err := s.MarkFailed(ctx, "r1", 1, "late", now); err != nil || committed {
		t.Fatalf("old-attempt MarkFailed: committed=%v err=%v", committed, err)
	}
	// an ordinary failed attempt is retried and re-adopts normally.
	if committed, err := s.MarkFailed(ctx, "r1", 2, "boom", now); err != nil || !committed {
		t.Fatalf("owner MarkFailed: committed=%v err=%v", committed, err)
	}
	if decision, _, err := s.Adopt(ctx, queuedStatus("r1", "alice", now), 3); err != nil || decision != rjobs.AdoptExecute {
		t.Fatalf("normal retry must execute: decision=%v err=%v", decision, err)
	}
}

func TestCommitTerminalFenceAndIdempotence(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	skel := queuedStatus("r1", "alice", now)
	if _, _, err := s.Reserve(ctx, skel, "k"); err != nil {
		t.Fatal(err)
	}
	if _, adopted, err := s.Adopt(ctx, skel, 1); err != nil {
		t.Fatal(err)
	} else {
		skel.StartedAt = adopted.StartedAt
	}

	terminal := func(state rjobs.State, attempt int) rjobs.Status {
		finished := now.Add(time.Minute)
		return rjobs.Status{
			RunID: "r1", Job: "export", State: state, Attempt: attempt,
			EnqueuedAt: now, StartedAt: skel.StartedAt, FinishedAt: &finished,
		}
	}

	// cancel wins the race first; the later success must not resurrect/overwrite.
	if committed, err := s.CommitTerminal(ctx, terminal(rjobs.StateCancelled, 1)); err != nil || !committed {
		t.Fatalf("cancel commit: committed=%v err=%v", committed, err)
	}
	if committed, err := s.CommitTerminal(ctx, terminal(rjobs.StateSucceeded, 1)); err != nil || committed {
		t.Fatalf("late success after cancel: committed=%v err=%v", committed, err)
	}
	got, _ := s.Get(ctx, "r1")
	if got.State != rjobs.StateCancelled {
		t.Fatalf("state = %q, want cancelled", got.State)
	}
	// RequestCancel is idempotent on the terminal row.
	st, err := s.RequestCancel(ctx, "r1")
	if err != nil || st.State != rjobs.StateCancelled || st.CancelRequested {
		t.Fatalf("terminal cancel changed the row: %+v %v", st, err)
	}
}

func TestSuccessCommitLosesToRecordedCancel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	skel := queuedStatus("r1", "alice", now)
	if _, _, err := s.Reserve(ctx, skel, "k"); err != nil {
		t.Fatal(err)
	}
	if _, adopted, err := s.Adopt(ctx, skel, 1); err != nil {
		t.Fatal(err)
	} else {
		skel.StartedAt = adopted.StartedAt
	}

	// cancel lands while the job is finishing and records StateCancelling.
	if _, err := s.RequestCancel(ctx, "r1"); err != nil {
		t.Fatal(err)
	}

	// the job ignores its context and returns success: the durable cancel
	// intent wins, so the success commit is rejected.
	finished := now.Add(time.Minute)
	committed, err := s.CommitTerminal(ctx, rjobs.Status{
		RunID: "r1", Job: "export", State: rjobs.StateSucceeded, Attempt: 1,
		EnqueuedAt: now, StartedAt: skel.StartedAt, FinishedAt: &finished,
		Result: rjobs.Params{"late": true},
	})
	if err != nil || committed {
		t.Fatalf("success over cancelling must be rejected: committed=%v err=%v", committed, err)
	}
	got, _ := s.Get(ctx, "r1")
	if got.State != rjobs.StateCancelling || got.Result != nil {
		t.Fatalf("row must stay in cancel: %+v", got)
	}

	// the cancel commit itself finalises, from running or cancelling.
	committed, err = s.CommitTerminal(ctx, rjobs.Status{
		RunID: "r1", Job: "export", State: rjobs.StateCancelled, Attempt: 1,
		EnqueuedAt: now, StartedAt: skel.StartedAt, FinishedAt: &finished,
	})
	if err != nil || !committed {
		t.Fatalf("cancel commit: committed=%v err=%v", committed, err)
	}
	got, _ = s.Get(ctx, "r1")
	if got.State != rjobs.StateCancelled {
		t.Fatalf("state = %q, want cancelled", got.State)
	}
}

func TestRequestCancel(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if err := s.Put(ctx, rjobs.Status{
		RunID: "run-1", Job: "j", State: rjobs.StateRunning, Attempt: 1, EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	st, err := s.RequestCancel(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !st.CancelRequested {
		t.Error("CancelRequested should be set after RequestCancel")
	}
	if st.State != rjobs.StateCancelling {
		t.Errorf("state = %q, want cancelling", st.State)
	}

	// a lifecycle write must not clobber the cancel intent.
	if err := s.Put(ctx, rjobs.Status{
		RunID: "run-1", Job: "j", State: rjobs.StateRunning, Attempt: 1, EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if !got.CancelRequested {
		t.Error("a Put must not clear CancelRequested")
	}
}

func TestRequestCancelTerminalIsNoop(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	if err := s.Put(ctx, rjobs.Status{
		RunID: "done", Job: "j", State: rjobs.StateSucceeded, Attempt: 1, EnqueuedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	st, err := s.RequestCancel(ctx, "done")
	if err != nil {
		t.Fatal(err)
	}
	if st.State != rjobs.StateSucceeded || st.CancelRequested {
		t.Errorf("cancel of a terminal run must be a no-op, got state=%q cancel=%v", st.State, st.CancelRequested)
	}

	if _, err := s.RequestCancel(ctx, "missing"); err == nil {
		t.Error("expected NotFound cancelling an unknown run")
	}
}
