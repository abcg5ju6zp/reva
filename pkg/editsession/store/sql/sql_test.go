// Copyright 2018-2026 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a License at
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

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
	"github.com/cs3org/reva/v3/pkg/errtypes"
)

func testResource() editsession.ResourceID {
	return editsession.ResourceID{StorageID: "stor-1", SpaceID: "space-1", OpaqueID: "inode-1"}
}

func newTestStore(t *testing.T) (editsession.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "editsession.db")
	st, err := New(context.Background(), map[string]any{
		"db_engine": "sqlite",
		"db_name":   path,
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return st, path
}

func reopenStore(t *testing.T, path string) editsession.Store {
	t.Helper()
	st, err := New(context.Background(), map[string]any{
		"db_engine": "sqlite",
		"db_name":   path,
	})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	return st
}

func newGeneration(seq int64, r editsession.ResourceID, state editsession.GenState) *editsession.Generation {
	now := time.Now()
	return &editsession.Generation{
		Sequence:      seq,
		Resource:      r,
		OldPath:       "/docs/file.docx",
		NewPath:       "/archive/file.docx",
		State:         state,
		Confirmations: map[editsession.Party]bool{},
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func TestCreateAndGetGeneration(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	g := newGeneration(1, r, editsession.GenActive)
	if err := st.CreateGeneration(ctx, g); err != nil {
		t.Fatalf("create generation: %v", err)
	}
	// duplicate sequence is rejected
	if err := st.CreateGeneration(ctx, g); err == nil {
		t.Fatal("expected error creating duplicate generation")
	}

	got, err := st.GetGeneration(ctx, r, 1)
	if err != nil {
		t.Fatalf("get generation: %v", err)
	}
	if got.Resource != r || got.State != editsession.GenActive || got.OldPath != "/docs/file.docx" {
		t.Fatalf("unexpected generation: %+v", got)
	}

	if _, err := st.GetGeneration(ctx, r, 99); err == nil {
		t.Fatal("expected error getting missing generation")
	} else if _, ok := err.(errtypes.IsNotFound); !ok {
		t.Fatalf("expected NotFound, got: %v", err)
	}
}

func TestListGenerationsOrdered(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenActive)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.CreateGeneration(ctx, newGeneration(2, r, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}

	gens, err := st.ListGenerations(ctx, r)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(gens) != 2 || gens[0].Sequence != 1 || gens[1].Sequence != 2 {
		t.Fatalf("unexpected order: %+v", gens)
	}
}

func TestTransitionGeneration(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()
	if err := st.CreateGeneration(ctx, newGeneration(2, r, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := st.TransitionGeneration(ctx, r, 2,
		[]editsession.GenState{editsession.GenFreezing}, editsession.GenMoving)
	if err != nil {
		t.Fatalf("transition to moving: %v", err)
	}
	if got.State != editsession.GenMoving {
		t.Fatalf("unexpected state: %s", got.State)
	}

	got, err = st.TransitionGeneration(ctx, r, 2,
		[]editsession.GenState{editsession.GenMoving}, editsession.GenConfirmed)
	if err != nil {
		t.Fatalf("transition to confirmed: %v", err)
	}
	if got.State != editsession.GenConfirmed || got.CommittedAt == nil {
		t.Fatalf("expected confirmed with committed_at: %+v", got)
	}
}

func TestIllegalTransitions(t *testing.T) {
	cases := []struct {
		name  string
		state editsession.GenState
		to    editsession.GenState
	}{
		{"active cannot move", editsession.GenActive, editsession.GenFreezing},
		{"freezing cannot skip to confirmed", editsession.GenFreezing, editsession.GenConfirmed},
		{"moving cannot go back", editsession.GenMoving, editsession.GenFreezing},
		{"confirmed is terminal", editsession.GenConfirmed, editsession.GenMoving},
		{"aborted is terminal", editsession.GenAborted, editsession.GenMoving},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, _ := newTestStore(t)
			ctx := context.Background()
			r := testResource()
			if err := st.CreateGeneration(ctx, newGeneration(1, r, c.state)); err != nil {
				t.Fatalf("create: %v", err)
			}
			if _, err := st.TransitionGeneration(ctx, r, 1,
				[]editsession.GenState{c.state}, c.to); err == nil {
				t.Fatalf("expected error transitioning %s -> %s", c.state, c.to)
			}
			// state unchanged
			got, err := st.GetGeneration(ctx, r, 1)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if got.State != c.state {
				t.Fatalf("dirty write: state changed to %s", got.State)
			}
		})
	}
}

func TestTransitionWrongFromOrMissing(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()
	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}
	// wrong from-state
	if _, err := st.TransitionGeneration(ctx, r, 1,
		[]editsession.GenState{editsession.GenMoving}, editsession.GenConfirmed); err == nil {
		t.Fatal("expected error on mismatched from-state")
	}
	// unknown generation
	if _, err := st.TransitionGeneration(ctx, r, 42,
		[]editsession.GenState{editsession.GenFreezing}, editsession.GenMoving); err == nil {
		t.Fatal("expected error on missing generation")
	}
}

func TestConfirmation(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	// confirmation requires moving state
	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.RecordConfirmation(ctx, r, 1, editsession.PartyLock); err == nil {
		t.Fatal("expected error confirming while freezing")
	}

	if _, err := st.TransitionGeneration(ctx, r, 1,
		[]editsession.GenState{editsession.GenFreezing}, editsession.GenMoving); err != nil {
		t.Fatalf("transition: %v", err)
	}
	if err := st.RecordConfirmation(ctx, r, 1, editsession.PartyLock); err != nil {
		t.Fatalf("record confirmation: %v", err)
	}
	// idempotent
	if err := st.RecordConfirmation(ctx, r, 1, editsession.PartyLock); err != nil {
		t.Fatalf("re-record confirmation: %v", err)
	}
	got, err := st.GetGeneration(ctx, r, 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !got.Confirmed(editsession.PartyLock) || got.Confirmed(editsession.PartyUpload) {
		t.Fatalf("unexpected confirmations: %+v", got.Confirmations)
	}
}

func TestInProgressGeneration(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	g, err := st.GetInProgressGeneration(ctx, r)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if g != nil {
		t.Fatalf("expected no in-progress generation, got %+v", g)
	}

	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenActive)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.CreateGeneration(ctx, newGeneration(2, r, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}
	g, err = st.GetInProgressGeneration(ctx, r)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if g == nil || g.Sequence != 2 {
		t.Fatalf("expected generation 2, got %+v", g)
	}

	if _, err := st.TransitionGeneration(ctx, r, 2,
		[]editsession.GenState{editsession.GenFreezing}, editsession.GenMoving); err != nil {
		t.Fatalf("transition: %v", err)
	}
	g, err = st.GetInProgressGeneration(ctx, r)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if g == nil || g.Sequence != 2 {
		t.Fatal("moving generation must still be in progress")
	}

	if _, err := st.TransitionGeneration(ctx, r, 2,
		[]editsession.GenState{editsession.GenMoving}, editsession.GenConfirmed); err != nil {
		t.Fatalf("transition: %v", err)
	}
	g, err = st.GetInProgressGeneration(ctx, r)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if g != nil {
		t.Fatalf("expected no in-progress generation after confirm, got %+v", g)
	}
}

func TestSessions(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	open := func(id string, gen int64) *editsession.Session {
		return &editsession.Session{
			ID:          id,
			Resource:    r,
			Generation:  gen,
			User:        &userpb.UserId{OpaqueId: "jdoe", Idp: "example.org"},
			DisplayName: "Jane Doe",
			AppProvider: "demo-app",
			LockID:      "lock-1",
			DownloadID:  "dl-1",
			UploadID:    "ul-1",
			State:       editsession.SessionActive,
			OpenedAt:    time.Now(),
		}
	}
	if err := st.CreateSession(ctx, open("s1", 1)); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := st.CreateSession(ctx, open("s2", 1)); err != nil {
		t.Fatalf("create session: %v", err)
	}

	got, err := st.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.User.GetOpaqueId() != "jdoe" || got.LockID != "lock-1" {
		t.Fatalf("unexpected session: %+v", got)
	}

	list, err := st.ListSessions(ctx, r, 0, false)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(list))
	}

	n, err := st.CountOpenSessions(ctx, r, 1)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 open, got %d", n)
	}

	if err := st.CloseSession(ctx, "s1", time.Now()); err != nil {
		t.Fatalf("close: %v", err)
	}
	// idempotent close
	if err := st.CloseSession(ctx, "s1", time.Now()); err != nil {
		t.Fatalf("reclose: %v", err)
	}
	// unknown id reports not found
	if err := st.CloseSession(ctx, "nope", time.Now()); err == nil {
		t.Fatal("expected error closing unknown session")
	}

	n, err = st.CountOpenSessions(ctx, r, 0)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 open after close, got %d", n)
	}

	openOnly, err := st.ListSessions(ctx, r, 1, true)
	if err != nil {
		t.Fatalf("list open only: %v", err)
	}
	if len(openOnly) != 1 || openOnly[0].ID != "s2" {
		t.Fatalf("unexpected open-only list: %+v", openOnly)
	}

	all, err := st.ListSessions(ctx, r, 0, false)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("closed sessions must remain listed, got %d", len(all))
	}
}

func TestUploads(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	u := &editsession.InFlightUpload{
		ID:         "up-1",
		Resource:   r,
		Generation: 1,
		SessionID:  "s1",
		State:      editsession.UploadOpen,
		StartedAt:  time.Now(),
	}
	if err := st.RegisterUpload(ctx, u); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := st.RegisterUpload(ctx, u); err == nil {
		t.Fatal("expected error registering duplicate upload")
	}

	open, err := st.ListOpenUploads(ctx, r, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(open) != 1 {
		t.Fatalf("expected 1 open upload, got %d", len(open))
	}

	if err := st.CompleteUpload(ctx, "up-1", time.Now()); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := st.CompleteUpload(ctx, "up-1", time.Now()); err != nil {
		t.Fatalf("re-complete should be idempotent: %v", err)
	}

	open, err = st.ListOpenUploads(ctx, r, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(open) != 0 {
		t.Fatalf("expected no open uploads, got %d", len(open))
	}

	if _, err := st.GetUpload(ctx, "up-1"); err != nil {
		t.Fatalf("get: %v", err)
	}
}

func TestAbortUpload(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()
	u := &editsession.InFlightUpload{
		ID: "up-1", Resource: r, Generation: 1, State: editsession.UploadOpen, StartedAt: time.Now(),
	}
	if err := st.RegisterUpload(ctx, u); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := st.AbortUpload(ctx, "up-1", time.Now()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if err := st.AbortUpload(ctx, "missing", time.Now()); err == nil {
		t.Fatal("expected error aborting missing upload")
	}
}

func TestSupersededMarkers(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()
	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenActive)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.MarkSuperseded(ctx, r, 1, 2); err != nil {
		t.Fatalf("mark: %v", err)
	}
	got, err := st.GetGeneration(ctx, r, 1)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.SupersededBy != 2 {
		t.Fatalf("expected superseded_by=2, got %d", got.SupersededBy)
	}
	if err := st.ClearSuperseded(ctx, r, 1); err != nil {
		t.Fatalf("clear: %v", err)
	}
	got, _ = st.GetGeneration(ctx, r, 1)
	if got.SupersededBy != 0 {
		t.Fatalf("expected superseded_by=0, got %d", got.SupersededBy)
	}
}

func TestListRecoverable(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r1 := editsession.ResourceID{StorageID: "stor-1", SpaceID: "s", OpaqueID: "a"}
	r2 := editsession.ResourceID{StorageID: "stor-1", SpaceID: "s", OpaqueID: "b"}

	if err := st.CreateGeneration(ctx, newGeneration(1, r1, editsession.GenActive)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.CreateGeneration(ctx, newGeneration(2, r1, editsession.GenFreezing)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.CreateGeneration(ctx, newGeneration(1, r2, editsession.GenMoving)); err != nil {
		t.Fatalf("create: %v", err)
	}

	got, err := st.ListRecoverableGenerations(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 recoverable generations, got %d", len(got))
	}
}

func TestDeleteGeneration(t *testing.T) {
	st, _ := newTestStore(t)
	ctx := context.Background()
	r := testResource()
	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenActive)); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := st.DeleteGeneration(ctx, r, 1); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := st.DeleteGeneration(ctx, r, 1); err == nil {
		t.Fatal("expected error deleting missing generation")
	}
}

func TestStatePersistsAcrossReopen(t *testing.T) {
	st, path := newTestStore(t)
	ctx := context.Background()
	r := testResource()

	if err := st.CreateGeneration(ctx, newGeneration(1, r, editsession.GenActive)); err != nil {
		t.Fatalf("create generation: %v", err)
	}
	if err := st.CreateGeneration(ctx, newGeneration(2, r, editsession.GenMoving)); err != nil {
		t.Fatalf("create generation: %v", err)
	}
	if err := st.RecordConfirmation(ctx, r, 2, editsession.PartyLock); err != nil {
		t.Fatalf("confirmation: %v", err)
	}
	if err := st.MarkSuperseded(ctx, r, 1, 2); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if err := st.CreateSession(ctx, &editsession.Session{
		ID: "s1", Resource: r, Generation: 1, State: editsession.SessionActive, OpenedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if err := st.RegisterUpload(ctx, &editsession.InFlightUpload{
		ID: "up-1", Resource: r, Generation: 1, State: editsession.UploadOpen, StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register upload: %v", err)
	}

	// reopen with a brand new store handle, as after a process restart
	st2 := reopenStore(t, path)

	gens, err := st2.ListGenerations(ctx, r)
	if err != nil {
		t.Fatalf("list generations: %v", err)
	}
	if len(gens) != 2 {
		t.Fatalf("expected 2 generations, got %d", len(gens))
	}
	g2 := gens[1]
	if g2.State != editsession.GenMoving || !g2.Confirmed(editsession.PartyLock) {
		t.Fatalf("generation state not restored: %+v", g2)
	}
	if gens[0].SupersededBy != 2 {
		t.Fatalf("superseded marker not restored: %d", gens[0].SupersededBy)
	}

	n, err := st2.CountOpenSessions(ctx, r, 0)
	if err != nil {
		t.Fatalf("count sessions: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 open session restored, got %d", n)
	}

	open, err := st2.ListOpenUploads(ctx, r, 0)
	if err != nil {
		t.Fatalf("list uploads: %v", err)
	}
	if len(open) != 1 || open[0].ID != "up-1" {
		t.Fatalf("in-flight upload not restored: %+v", open)
	}
}
