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

package editsession

import (
	"context"
	"errors"
	"testing"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

func testR() ResourceID {
	return ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "inode"}
}

func newRef(path string) *provider.Reference {
	return &provider.Reference{Path: path, ResourceId: testR().ToCS3()}
}

func TestMemoryAppProviderTakeOver(t *testing.T) {
	p := NewMemoryAppProvider()
	ctx := context.Background()
	r := testR()

	// unknown session fails
	if err := p.TakeOver(ctx, "s1", r, newRef("/new"), 2); err == nil {
		t.Fatal("expected error taking over unknown session")
	}

	p.OpenSession("s1", r)

	// wrong resource fails
	other := ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "other"}
	if err := p.TakeOver(ctx, "s1", other, newRef("/new"), 2); err == nil {
		t.Fatal("expected error taking over with wrong resource")
	}
	// nil new reference fails
	if err := p.TakeOver(ctx, "s1", r, nil, 2); err == nil {
		t.Fatal("expected error taking over without new reference")
	}

	if err := p.TakeOver(ctx, "s1", r, newRef("/new"), 2); err != nil {
		t.Fatalf("takeover: %v", err)
	}
	gen, ok := p.TakenOver("s1")
	if !ok || gen != 2 {
		t.Fatalf("expected takeover at gen 2, got %d/%v", gen, ok)
	}
	// idempotent at same generation
	if err := p.TakeOver(ctx, "s1", r, newRef("/new"), 2); err != nil {
		t.Fatalf("repeated takeover should be idempotent: %v", err)
	}
	// older generation rejected
	if err := p.TakeOver(ctx, "s1", r, newRef("/new"), 1); err == nil {
		t.Fatal("expected error taking over to older generation")
	}
}

func TestMemoryAppProviderSavedState(t *testing.T) {
	p := NewMemoryAppProvider()
	ctx := context.Background()
	r := testR()
	p.OpenSession("s1", r)

	saved, gen, err := p.QuerySaved(ctx, "s1", r)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if saved {
		t.Fatal("expected unsaved session")
	}

	if err := p.RecordSave("s1", 2); err != nil {
		t.Fatalf("record save: %v", err)
	}
	saved, gen, err = p.QuerySaved(ctx, "s1", r)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !saved || gen != 2 {
		t.Fatalf("expected saved at gen 2, got saved=%v gen=%d", saved, gen)
	}

	// unknown session query fails
	if _, _, err := p.QuerySaved(ctx, "nope", r); err == nil {
		t.Fatal("expected error querying unknown session")
	}
}

func TestMemoryAppProviderTakeoverErr(t *testing.T) {
	p := NewMemoryAppProvider()
	p.OpenSession("s1", testR())
	want := errors.New("app unavailable")
	p.TakeoverErr = want
	if err := p.TakeOver(context.Background(), "s1", testR(), newRef("/new"), 2); !errors.Is(err, want) {
		t.Fatalf("expected scripted error, got %v", err)
	}
	if _, ok := p.TakenOver("s1"); ok {
		t.Fatal("no takeover state must be recorded on failure")
	}
}

func TestFakeStorage(t *testing.T) {
	f := &FakeStorage{}
	old, nw := newRef("/old"), newRef("/new")
	if err := f.Move(context.Background(), old, nw); err != nil {
		t.Fatalf("move: %v", err)
	}
	moves := f.Moves()
	if len(moves) != 1 || moves[0].OldRef != old || moves[0].NewRef != nw {
		t.Fatalf("unexpected moves: %+v", moves)
	}

	want := errors.New("storage down")
	f.Err = want
	if err := f.Move(context.Background(), old, nw); !errors.Is(err, want) {
		t.Fatalf("expected scripted error, got %v", err)
	}
}

func TestFakeLockerLifecycle(t *testing.T) {
	f := NewFakeLocker()
	ctx := context.Background()
	old := newRef("/old")
	lock := &provider.Lock{LockId: "lock-1"}

	// missing lock reports not found
	if _, err := f.GetLock(ctx, old); err == nil {
		t.Fatal("expected not found")
	}
	// unlock missing fails
	if err := f.Unlock(ctx, old, lock); err == nil {
		t.Fatal("expected error unlocking missing lock")
	}

	if err := f.SetLock(ctx, old, lock); err != nil {
		t.Fatalf("set lock: %v", err)
	}
	if err := f.SetLock(ctx, old, lock); err == nil {
		t.Fatal("expected conflict double set lock")
	}
	if !f.HasLock(old) {
		t.Fatal("expected lock present")
	}
	got, err := f.GetLock(ctx, old)
	if err != nil || got.GetLockId() != "lock-1" {
		t.Fatalf("unexpected get: %v %+v", err, got)
	}

	// refresh with wrong existing id fails
	if err := f.RefreshLock(ctx, old, &provider.Lock{LockId: "lock-2"}, "wrong"); err == nil {
		t.Fatal("expected error on wrong existing lock id")
	}
	// good refresh updates the lock
	if err := f.RefreshLock(ctx, old, &provider.Lock{LockId: "lock-2"}, "lock-1"); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got, _ = f.GetLock(ctx, old)
	if got.GetLockId() != "lock-2" {
		t.Fatalf("expected refreshed lock id, got %s", got.GetLockId())
	}

	if err := f.Unlock(ctx, old, got); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if f.HasLock(old) {
		t.Fatal("expected lock removed")
	}
}

func TestFakeLockerHooks(t *testing.T) {
	f := NewFakeLocker()
	ref := newRef("/p")
	want := errors.New("refresh failed")
	f.RefreshLockHook = func(r *provider.Reference) error { return want }
	if err := f.RefreshLock(context.Background(), ref, &provider.Lock{}, "x"); !errors.Is(err, want) {
		t.Fatalf("expected hook error, got %v", err)
	}

	// calls are recorded
	calls := f.Calls()
	if len(calls) != 1 || calls[0].Op != OpRefreshLock {
		t.Fatalf("unexpected calls: %+v", calls)
	}
}
