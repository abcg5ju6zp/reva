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
	"sync"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/errtypes"
)

var (
	errNotFoundLock = errtypes.NotFound("editsession: lock not found")
	errConflictLock = errtypes.Conflict("editsession: reference already locked")
	errBadLock      = errtypes.BadRequest("editsession: existing lock id does not match")
)

// FakeMoveRecord is one recorded Move call.
type FakeMoveRecord struct {
	OldRef *provider.Reference
	NewRef *provider.Reference
}

// FakeStorage is a scriptable Storage. When Entries is non-nil it behaves as
// a tiny namespace: Move relocates the entry, Move to an already-relocated
// entry returns ErrAlreadyMoved, Stat probes entries. When Entries is nil it
// only records calls (used by tests that do not care about entry state).
type FakeStorage struct {
	mu sync.Mutex

	// Err, when non-nil, is returned by Move.
	Err error

	Entries map[string]ResourceID

	moves []FakeMoveRecord
}

// SeedEntry seeds an entry.
func (f *FakeStorage) SeedEntry(path string, r ResourceID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Entries == nil {
		f.Entries = map[string]ResourceID{}
	}
	f.Entries[path] = r
}

// Move implements Storage.
func (f *FakeStorage) Move(_ context.Context, oldRef, newRef *provider.Reference) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moves = append(f.moves, FakeMoveRecord{OldRef: oldRef, NewRef: newRef})
	if f.Err != nil {
		return f.Err
	}
	if f.Entries != nil {
		id, oldOK := f.Entries[oldRef.GetPath()]
		if !oldOK {
			// source absent: if the destination carries the same identity the
			// entry was already moved (resume case).
			if rid, err := ResourceIDFromCS3(oldRef.GetResourceId()); err == nil {
				if newID, newOK := f.Entries[newRef.GetPath()]; newOK && newID == rid {
					return ErrAlreadyMoved
				}
			}
			return errtypes.NotFound("editsession: source entry not present: " + oldRef.GetPath())
		}
		delete(f.Entries, oldRef.GetPath())
		f.Entries[newRef.GetPath()] = id
	}
	return nil
}

// Stat implements Storage.
func (f *FakeStorage) Stat(_ context.Context, ref *provider.Reference) (*provider.ResourceInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Entries == nil {
		return &provider.ResourceInfo{Path: ref.GetPath(), Id: ref.GetResourceId()}, nil
	}
	id, ok := f.Entries[ref.GetPath()]
	if !ok {
		return nil, errtypes.NotFound("editsession: entry not present: " + ref.GetPath())
	}
	return &provider.ResourceInfo{Path: ref.GetPath(), Id: id.ToCS3()}, nil
}

// Moves returns the recorded moves.
func (f *FakeStorage) Moves() []FakeMoveRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]FakeMoveRecord, len(f.moves))
	copy(out, f.moves)
	return out
}

// LockOp identifies a locker operation.
type LockOp string

// Locker operations.
const (
	OpGetLock     LockOp = "get_lock"
	OpSetLock     LockOp = "set_lock"
	OpRefreshLock LockOp = "refresh_lock"
	OpUnlock      LockOp = "unlock"
)

// FakeLockCall is one recorded locker call.
type FakeLockCall struct {
	Op   LockOp
	Ref  *provider.Reference
	Lock *provider.Lock
}

// FakeLocker is a scriptable Locker with per-path lock state. Hooks, when set,
// override the default behavior of the corresponding operation and can inject
// failures (e.g. a failing refresh during takeover).
type FakeLocker struct {
	mu sync.Mutex

	locks map[string]*provider.Lock // key -> lock

	GetLockHook     func(ref *provider.Reference) error
	SetLockHook     func(ref *provider.Reference) error
	RefreshLockHook func(ref *provider.Reference) error
	UnlockHook      func(ref *provider.Reference) error

	calls []FakeLockCall
}

// NewFakeLocker returns an empty FakeLocker.
func NewFakeLocker() *FakeLocker {
	return &FakeLocker{locks: map[string]*provider.Lock{}}
}

func lockKey(ref *provider.Reference) string {
	return ref.String()
}

// GetLock implements Locker.
func (f *FakeLocker) GetLock(_ context.Context, ref *provider.Reference) (*provider.Lock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, FakeLockCall{Op: OpGetLock, Ref: ref})
	if f.GetLockHook != nil {
		if err := f.GetLockHook(ref); err != nil {
			return nil, err
		}
	}
	lock, ok := f.locks[lockKey(ref)]
	if !ok {
		return nil, errNotFoundLock
	}
	return lock, nil
}

// SetLock implements Locker.
func (f *FakeLocker) SetLock(_ context.Context, ref *provider.Reference, lock *provider.Lock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, FakeLockCall{Op: OpSetLock, Ref: ref, Lock: lock})
	if f.SetLockHook != nil {
		if err := f.SetLockHook(ref); err != nil {
			return err
		}
	}
	if _, ok := f.locks[lockKey(ref)]; ok {
		return errConflictLock
	}
	f.locks[lockKey(ref)] = lock
	return nil
}

// RefreshLock implements Locker: the existing lock must be present and carry
// the expected lock id.
func (f *FakeLocker) RefreshLock(_ context.Context, ref *provider.Reference, lock *provider.Lock, existingLockID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, FakeLockCall{Op: OpRefreshLock, Ref: ref, Lock: lock})
	if f.RefreshLockHook != nil {
		if err := f.RefreshLockHook(ref); err != nil {
			return err
		}
	}
	existing, ok := f.locks[lockKey(ref)]
	if !ok {
		return errNotFoundLock
	}
	if existing.GetLockId() != existingLockID {
		return errBadLock
	}
	f.locks[lockKey(ref)] = lock
	return nil
}

// Unlock implements Locker.
func (f *FakeLocker) Unlock(_ context.Context, ref *provider.Reference, lock *provider.Lock) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, FakeLockCall{Op: OpUnlock, Ref: ref, Lock: lock})
	if f.UnlockHook != nil {
		if err := f.UnlockHook(ref); err != nil {
			return err
		}
	}
	if _, ok := f.locks[lockKey(ref)]; !ok {
		return errNotFoundLock
	}
	delete(f.locks, lockKey(ref))
	return nil
}

// HasLock reports whether a lock is currently held at the reference.
func (f *FakeLocker) HasLock(ref *provider.Reference) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.locks[lockKey(ref)]
	return ok
}

// Calls returns the recorded locker calls.
func (f *FakeLocker) Calls() []FakeLockCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]FakeLockCall, len(f.calls))
	copy(out, f.calls)
	return out
}
