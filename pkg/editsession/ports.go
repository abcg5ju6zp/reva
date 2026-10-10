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
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// Storage is the directory-entry port: it performs the actual move on the
// storage provider and probes entry state when resuming.
type Storage interface {
	// Move switches the directory entry from oldRef to newRef. When the entry
	// is already at the destination it returns ErrAlreadyMoved.
	Move(ctx context.Context, oldRef, newRef *provider.Reference) error
	// Stat probes whether a reference currently exists, used to tell a moved
	// entry from an unmoved one during recovery.
	Stat(ctx context.Context, ref *provider.Reference) (*provider.ResourceInfo, error)
}

// Locker is the storage lock port used during takeover. The lock at the old
// location stays valid until a lock at the new reference is confirmed.
type Locker interface {
	GetLock(ctx context.Context, ref *provider.Reference) (*provider.Lock, error)
	SetLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error
	RefreshLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock, existingLockID string) error
	Unlock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error
}

// AppProvider is the application-side takeover port (the in-repo counterpart
// of what the external wopiserver would implement).
type AppProvider interface {
	// TakeOver asks the application to relocate the session's editing
	// context to the new reference.
	TakeOver(ctx context.Context, sessionID string, r ResourceID, newRef *provider.Reference, generation int64) error
	// QuerySaved reports whether the session's document is saved for the
	// resource and at which generation, used to reconcile an ambiguous
	// callback timeout.
	QuerySaved(ctx context.Context, sessionID string, r ResourceID) (saved bool, savedGeneration int64, err error)
	// ConfirmReference is the provider-level confirmation that the new
	// reference is valid (one call per generation, after all sessions were
	// taken over).
	ConfirmReference(ctx context.Context, r ResourceID, newRef *provider.Reference, generation int64) error
}

// EndpointIssuer re-issues the download and upload endpoints for a session
// against a new path during takeover.
type EndpointIssuer interface {
	ReissueDownload(ctx context.Context, s *Session, ref *provider.Reference) (endpointID string, err error)
	ReissueUpload(ctx context.Context, s *Session, ref *provider.Reference) (endpointID string, err error)
}

// Clock abstracts time.
type Clock interface {
	Now() time.Time
}

// SystemClock is the default clock.
type SystemClock struct{}

// Now returns the current time.
func (SystemClock) Now() time.Time { return time.Now() }
