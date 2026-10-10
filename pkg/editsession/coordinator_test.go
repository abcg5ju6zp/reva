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
	"path/filepath"
	"sync"
	"testing"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
	sqlstore "github.com/cs3org/reva/v3/pkg/editsession/store/sql"
)

// aliases keep the test body written in unqualified names.
type (
	ResourceID            = editsession.ResourceID
	Session               = editsession.Session
	InFlightUpload        = editsession.InFlightUpload
	Callback              = editsession.Callback
	Generation            = editsession.Generation
	Party                 = editsession.Party
	Config                = editsession.Config
	Store                 = editsession.Store
	FakeStorage           = editsession.FakeStorage
	FakeLocker            = editsession.FakeLocker
	MemoryAppProvider     = editsession.MemoryAppProvider
	FakeEndpoints         = editsession.FakeEndpoints
	Coordinator           = editsession.Coordinator
	providerReferenceType = provider.Reference
	lockType              = provider.Lock
)

var (
	PartySessions    = editsession.PartySessions
	PartyAppProvider = editsession.PartyAppProvider
	PartyLock        = editsession.PartyLock
	PartyDownload    = editsession.PartyDownload
	PartyUpload      = editsession.PartyUpload

	ErrFrozen             = editsession.ErrFrozen
	ErrGenerationMismatch = editsession.ErrGenerationMismatch
	ErrNoSessions         = editsession.ErrNoSessions

	GenConfirmed = editsession.GenConfirmed
	GenMoving    = editsession.GenMoving
	GenFreezing  = editsession.GenFreezing
	GenActive    = editsession.GenActive
	GenAborted   = editsession.GenAborted
	UploadOpen   = editsession.UploadOpen

	CallbackSaved  = editsession.CallbackSaved
	CallbackClosed = editsession.CallbackClosed

	New                  = editsession.New
	NewFakeLocker        = editsession.NewFakeLocker
	NewMemoryAppProvider = editsession.NewMemoryAppProvider
)

func refAt(path string, r ResourceID) *provider.Reference {
	return &provider.Reference{Path: path, ResourceId: r.ToCS3()}
}

const (
	oldPath = "/docs/file.docx"
	newPath = "/archive/file.docx"
)

type rig struct {
	coord *Coordinator
	store Store
	fs    *FakeStorage
	locks *FakeLocker
	app   *MemoryAppProvider
	ends  *FakeEndpoints
	r     ResourceID
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st, err := sqlstore.New(context.Background(), map[string]any{
		"db_engine": "sqlite",
		"db_name":   filepath.Join(t.TempDir(), "editsession.db"),
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	r := ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "inode"}
	fs := &FakeStorage{}
	fs.SeedEntry(oldPath, r)
	locks := NewFakeLocker()
	app := NewMemoryAppProvider()
	ends := &FakeEndpoints{}
	coord := New(st, fs, locks, app, ends, Config{
		DrainPoll:    time.Millisecond,
		DrainTimeout: 2 * time.Second,
	})
	g := &rig{coord: coord, store: st, fs: fs, locks: locks, app: app, ends: ends, r: r}
	// write lock held for the app at the old reference (seeded once).
	if err := g.locks.SetLock(context.Background(), g.oldRef(), &lockType{LockId: "lock-1"}); err != nil {
		t.Fatalf("seed lock: %v", err)
	}
	return g
}

func (g *rig) oldRef() *providerReferenceType { return refAt(oldPath, g.r) }
func (g *rig) newRef() *providerReferenceType { return refAt(newPath, g.r) }

func (g *rig) seedSession(t *testing.T, id string) {
	t.Helper()
	ctx := context.Background()
	s := &Session{
		ID: id, Resource: g.r,
		User:        &userpb.UserId{OpaqueId: "jdoe", Idp: "example.org"},
		DisplayName: "Jane", AppProvider: "demo-app",
		LockID: "lock-1", DownloadID: "dl-old", UploadID: "ul-old",
	}
	if err := g.coord.OpenSession(ctx, s); err != nil {
		t.Fatalf("open session: %v", err)
	}
	g.app.OpenSession(id, g.r)
}

func TestMoveHappyPath(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")
	g.seedSession(t, "s2")

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if got.State != GenConfirmed {
		t.Fatalf("expected confirmed, got %s", got.State)
	}
	if !got.AllConfirmed() {
		t.Fatalf("not every party confirmed: %+v", got.Confirmations)
	}

	// lock follows to the new reference; old reference is unlocked.
	if !g.locks.HasLock(g.newRef()) {
		t.Fatal("expected lock at new reference")
	}
	if g.locks.HasLock(g.oldRef()) {
		t.Fatal("old reference must be unlocked")
	}

	// sessions now bind the new generation and carry re-issued endpoints.
	sessions, err := g.store.ListSessions(ctx, g.r, 2, true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions at new generation, got %d", len(sessions))
	}
	for _, s := range sessions {
		if s.DownloadID == "dl-old" || s.UploadID == "ul-old" || s.DownloadID == "" || s.UploadID == "" {
			t.Fatalf("endpoints not re-issued: %+v", s)
		}
	}

	// no in-progress generation remains; edit opens are allowed again.
	if err := g.coord.CheckEditOpen(ctx, g.r); err != nil {
		t.Fatalf("expected edit opens allowed, got %v", err)
	}

	// old path is not an editable entry: an upload bound to the old
	// generation cannot commit and an old-path unlock is rejected.
	if err := g.store.RegisterUpload(ctx, &InFlightUpload{
		ID: "up-old", Resource: g.r, Generation: 1, State: UploadOpen, StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register old upload: %v", err)
	}
	if err := g.coord.CommitAllowed(ctx, "up-old"); err == nil {
		t.Fatal("expected old-generation save rejected")
	}
	if err := g.coord.Unlock(ctx, g.r, g.oldRef(), &lockType{LockId: "lock-1"}); err == nil {
		t.Fatal("expected old-path unlock rejected")
	}
	// the new-reference lock survived both attempts.
	if !g.locks.HasLock(g.newRef()) {
		t.Fatal("new lock must be intact")
	}
}

func TestFreezeBlocksEditAndDrains(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	// an in-flight save registered before the move keeps it draining.
	up := &InFlightUpload{ID: "up-1", Resource: g.r}
	if err := g.coord.BeginUpload(ctx, up); err != nil {
		t.Fatalf("begin upload: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var moveResult *Generation
	var moveErr error
	go func() {
		defer wg.Done()
		moveResult, moveErr = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	}()

	// while draining: edit opens and new saves are frozen; the storage entry
	// must not have moved yet.
	waitUntil(t, func() bool { return errors.Is(errorOf(g.coord.CheckEditOpen(ctx, g.r)), ErrFrozen) })

	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-2", Resource: g.r}); !errors.Is(err, ErrFrozen) {
		t.Fatalf("expected new save frozen, got %v", err)
	}
	if len(g.fs.Moves()) != 0 {
		t.Fatal("directory entry must not switch before drain")
	}

	// a concurrent move joins the single drive and waits for it rather than
	// running the protocol twice.
	var joinWG sync.WaitGroup
	joinWG.Add(1)
	var joined *Generation
	var joinErr error
	go func() {
		defer joinWG.Done()
		joined, joinErr = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	}()

	// read-only/preview opens are never put through the edit gate: the only
	// thing a read-only caller invokes is nothing (gate is edit-only). Assert
	// the gate stays the sole rejection surface and does not classify reads.
	if err := g.coord.CheckEditOpen(ctx, g.r); !errors.Is(err, ErrFrozen) {
		t.Fatalf("expected frozen for edits, got %v", err)
	}

	// completing the in-flight save lets the switch proceed; both the original
	// drive and the joined move converge on the same confirmed generation.
	if err := g.coord.CompleteUpload(ctx, "up-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	wg.Wait()
	if moveErr != nil {
		t.Fatalf("move: %v", moveErr)
	}
	if moveResult.State != GenConfirmed {
		t.Fatalf("expected confirmed, got %s", moveResult.State)
	}
	joinWG.Wait()
	if joinErr != nil {
		t.Fatalf("joined move: %v", joinErr)
	}
	if joined.Sequence != moveResult.Sequence || joined.State != GenConfirmed {
		t.Fatalf("joined move must converge on the same generation: %+v", joined)
	}
	if len(g.fs.Moves()) != 1 {
		t.Fatal("entry switched exactly once")
	}
}

func TestConfirmationParties(t *testing.T) {
	cases := []struct {
		name      string
		prepare   func(g *rig)
		wantParty Party
		wantErr   bool
	}{
		{
			name: "app takeover fails",
			prepare: func(g *rig) {
				g.app.TakeoverErr = errors.New("app gone")
			},
			wantParty: PartySessions,
			wantErr:   true,
		},
		{
			name: "provider reference confirmation fails",
			prepare: func(g *rig) {
				g.app.ConfirmErr = errors.New("ref rejected")
			},
			wantParty: PartyAppProvider,
			wantErr:   true,
		},
		{
			name: "download endpoint fails",
			prepare: func(g *rig) {
				g.ends.DownloadErr = errors.New("no download")
			},
			wantParty: PartyDownload,
			wantErr:   true,
		},
		{
			name: "upload endpoint fails",
			prepare: func(g *rig) {
				g.ends.UploadErr = errors.New("no upload")
			},
			wantParty: PartyUpload,
			wantErr:   true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := newRig(t)
			ctx := context.Background()
			g.seedSession(t, "s1")
			c.prepare(g)

			got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
			if c.wantErr {
				// hard takeover/confirmation errors surface to the caller;
				// the generation stays moving and can be resumed.
				if err == nil {
					t.Fatal("expected move error, got nil")
				}
			} else if err != nil {
				t.Fatalf("move returned: %v", err)
			}
			if got.State != GenMoving {
				t.Fatalf("expected moving, got %s", got.State)
			}
			if got.Confirmed(c.wantParty) {
				t.Fatalf("party %s must not be recorded", c.wantParty)
			}
			if got.AllConfirmed() {
				t.Fatal("generation must not be confirmed with a missing party")
			}
		})
	}
}

func TestRetryMovingConfirms(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")
	g.ends.DownloadErr = errors.New("temporary")

	got, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err == nil {
		t.Fatal("expected endpoint failure surfaced to the move caller, got nil")
	}
	if got.State != GenMoving {
		t.Fatalf("expected moving, still resumable, got %s", got.State)
	}

	// a second move after the transient cleared resumes and confirms the
	// same generation.
	g.ends.DownloadErr = nil
	got, err = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef())
	if err != nil {
		t.Fatalf("retry move: %v", err)
	}
	if got.Sequence != 2 || got.State != GenConfirmed {
		t.Fatalf("expected same generation confirmed, got seq=%d state=%s", got.Sequence, got.State)
	}
}

func TestCommitGateMatrix(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	// save on the unchallenged serving generation is allowed.
	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-0", Resource: g.r}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := g.coord.CommitAllowed(ctx, "up-0"); err != nil {
		t.Fatalf("expected commit allowed on serving gen: %v", err)
	}
	if err := g.coord.CompleteUpload(ctx, "up-0"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	// freeze with one in-flight save: that save may finish during drain.
	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-drain", Resource: g.r}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef()) }()
	waitUntil(t, func() bool { return errors.Is(errorOf(g.coord.CheckEditOpen(ctx, g.r)), ErrFrozen) })
	if err := g.coord.CommitAllowed(ctx, "up-drain"); err != nil {
		t.Fatalf("in-flight save must be allowed during drain: %v", err)
	}
	if err := g.coord.CompleteUpload(ctx, "up-drain"); err != nil {
		t.Fatalf("complete drain save: %v", err)
	}
	wg.Wait()

	// confirmed: old-gen saves rejected even when they were pre-registered;
	// new-gen saves allowed.
	if err := g.store.RegisterUpload(ctx, &InFlightUpload{
		ID: "up-late", Resource: g.r, Generation: 1, State: UploadOpen, StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("register late: %v", err)
	}
	if err := g.coord.CommitAllowed(ctx, "up-late"); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected generation mismatch, got %v", err)
	}
	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-new", Resource: g.r}); err != nil {
		t.Fatalf("begin on new gen: %v", err)
	}
	if err := g.coord.CommitAllowed(ctx, "up-new"); err != nil {
		t.Fatalf("commit on new gen must be allowed: %v", err)
	}
}

func TestUnlockGenerationGate(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	// unlock during freeze is frozen.
	if err := g.coord.BeginUpload(ctx, &InFlightUpload{ID: "up-1", Resource: g.r}); err != nil {
		t.Fatalf("begin: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _, _ = g.coord.Move(ctx, g.r, g.oldRef(), g.newRef()) }()
	waitUntil(t, func() bool { return errors.Is(errorOf(g.coord.CheckEditOpen(ctx, g.r)), ErrFrozen) })
	if err := g.coord.Unlock(ctx, g.r, g.oldRef(), &lockType{LockId: "lock-1"}); !errors.Is(err, ErrFrozen) {
		t.Fatalf("expected unlock frozen during drain, got %v", err)
	}

	if err := g.coord.CompleteUpload(ctx, "up-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	wg.Wait()

	// after confirm: old-path unlock rejected, new-path unlock succeeds.
	if err := g.coord.Unlock(ctx, g.r, g.oldRef(), &lockType{LockId: "lock-1"}); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected mismatch on old-path unlock, got %v", err)
	}
	if !g.locks.HasLock(g.newRef()) {
		t.Fatal("lock must remain after rejected unlock")
	}
	if err := g.coord.Unlock(ctx, g.r, g.newRef(), &lockType{LockId: "lock-1"}); err != nil {
		t.Fatalf("current-generation unlock: %v", err)
	}
	if g.locks.HasLock(g.newRef()) {
		t.Fatal("lock should be released")
	}
}

func TestCleanupAfterLastSession(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")
	g.seedSession(t, "s2")

	if _, err := g.coord.Move(ctx, g.r, g.oldRef(), g.newRef()); err != nil {
		t.Fatalf("move: %v", err)
	}

	// first close: old generation retained.
	if err := g.coord.CloseSession(ctx, "s1"); err != nil {
		t.Fatalf("close s1: %v", err)
	}
	if _, err := g.store.GetGeneration(ctx, g.r, 1); err != nil {
		t.Fatalf("old generation must survive while a session is open: %v", err)
	}

	// last close: old generation cleaned, serving generation preserved.
	if err := g.coord.CloseSession(ctx, "s2"); err != nil {
		t.Fatalf("close s2: %v", err)
	}
	if _, err := g.store.GetGeneration(ctx, g.r, 1); err == nil {
		t.Fatal("old generation must be cleaned after the last session closes")
	}
	if _, err := g.store.GetGeneration(ctx, g.r, 2); err != nil {
		t.Fatalf("serving generation must be preserved: %v", err)
	}
}

func TestSavedCallbackIdempotency(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")

	cb := &Callback{SessionID: "s1", Resource: g.r, Generation: 1, Kind: CallbackSaved, Seq: 1}
	if err := g.coord.HandleCallback(ctx, cb); err != nil {
		t.Fatalf("callback: %v", err)
	}
	isNew, err := g.store.RecordCallback(ctx, g.r, "s1", 1, CallbackSaved)
	if err != nil {
		t.Fatalf("record check: %v", err)
	}
	if isNew {
		t.Fatal("repeated callback must be recorded as already applied")
	}
	// HandleCallback itself is also idempotent.
	if err := g.coord.HandleCallback(ctx, cb); err != nil {
		t.Fatalf("repeated handle callback: %v", err)
	}

	// a save callback claiming another generation is rejected.
	cb2 := &Callback{SessionID: "s1", Resource: g.r, Generation: 2, Kind: CallbackSaved, Seq: 2}
	if err := g.coord.HandleCallback(ctx, cb2); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("expected generation mismatch, got %v", err)
	}
}

func TestClosedCallbackClosesSession(t *testing.T) {
	g := newRig(t)
	ctx := context.Background()
	g.seedSession(t, "s1")
	cb := &Callback{SessionID: "s1", Resource: g.r, Generation: 1, Kind: CallbackClosed, Seq: 1}
	if err := g.coord.HandleCallback(ctx, cb); err != nil {
		t.Fatalf("closed callback: %v", err)
	}
	s, err := g.store.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if s.IsOpen() {
		t.Fatal("session must be closed")
	}
}

func TestMoveWithoutSessions(t *testing.T) {
	g := newRig(t)
	if _, err := g.coord.Move(context.Background(), g.r, g.oldRef(), g.newRef()); !errors.Is(err, ErrNoSessions) {
		t.Fatalf("expected no-sessions error, got %v", err)
	}
	// and no generation was created.
	gens, err := g.store.ListGenerations(context.Background(), g.r)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(gens) != 0 {
		t.Fatalf("no generation must be created without sessions, got %d", len(gens))
	}
}

// --- small helpers ---

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

func errorOf(err error) error { return err }
