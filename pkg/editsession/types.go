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

// Package editsession tracks read-write editing sessions and the editing
// generations that are created when an actively edited resource is moved or
// renamed. State is keyed by the stable resource identity; a move inside one
// storage provider does not change that identity, but opens a new generation
// that the sessions, the app provider, the storage lock and the
// download/upload endpoints must all confirm before it is served.
package editsession

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// ResourceID is the stable identity of a resource. Moves and renames inside
// one storage provider keep it unchanged; editing sessions and generations
// are keyed by it.
type ResourceID struct {
	StorageID string
	SpaceID   string
	OpaqueID  string
}

// ResourceIDFromCS3 converts a CS3 ResourceId.
func ResourceIDFromCS3(id *provider.ResourceId) (ResourceID, error) {
	if id == nil {
		return ResourceID{}, fmt.Errorf("editsession: nil resource id")
	}
	if id.GetStorageId() == "" || id.GetOpaqueId() == "" {
		return ResourceID{}, fmt.Errorf("editsession: incomplete resource id: %s", id.String())
	}
	return ResourceID{
		StorageID: id.GetStorageId(),
		SpaceID:   id.GetSpaceId(),
		OpaqueID:  id.GetOpaqueId(),
	}, nil
}

// ToCS3 converts back to a CS3 ResourceId.
func (r ResourceID) ToCS3() *provider.ResourceId {
	return &provider.ResourceId{
		StorageId: r.StorageID,
		SpaceId:   r.SpaceID,
		OpaqueId:  r.OpaqueID,
	}
}

// Key is the stable string key under which the identity is stored. The triple
// is NUL-joined and base64-URL encoded, so ID components may contain any
// character without key ambiguity.
func (r ResourceID) Key() string {
	raw := strings.Join([]string{r.StorageID, r.SpaceID, r.OpaqueID}, "\x00")
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func (r ResourceID) String() string { return r.Key() }

// GenState is the state of an editing generation.
type GenState string

const (
	// GenActive is the state of the serving generation (the base generation
	// for a resource, or the generation currently served after a move).
	GenActive GenState = "active"
	// GenFreezing: new edit opens are frozen and in-flight saves are drained,
	// the directory entry has not yet switched.
	GenFreezing GenState = "freezing"
	// GenMoving: the directory entry switched (or is switching); sessions,
	// app provider, lock and endpoints are taking over the new reference.
	GenMoving GenState = "moving"
	// GenConfirmed: all parties confirmed the new reference; the generation
	// is now the one edit tokens are issued for.
	GenConfirmed GenState = "confirmed"
	// GenAborted: the move was rolled back; the generation is discarded and
	// the previous generation keeps serving.
	GenAborted GenState = "aborted"
)

// CanTransitionTo reports whether moving to the given state is legal.
// Freezing and moving generations only move forward, or abort; confirmed and
// aborted generations are terminal. A base (active) generation never changes
// its state (only its SupersededBy marker).
func (s GenState) CanTransitionTo(to GenState) bool {
	switch s {
	case GenFreezing:
		return to == GenMoving || to == GenAborted
	case GenMoving:
		return to == GenConfirmed || to == GenAborted
	default:
		return false
	}
}

// IsInProgress reports whether the generation still has work that a restart
// must resume.
func (s GenState) IsInProgress() bool {
	return s == GenFreezing || s == GenMoving
}

// Party is one of the parties that must confirm the new reference.
type Party string

const (
	// PartySessions: every active editing session took over the new reference.
	PartySessions Party = "sessions"
	// PartyAppProvider: the app provider confirmed the new reference.
	PartyAppProvider Party = "app_provider"
	// PartyLock: the storage write lock is confirmed at the new reference.
	PartyLock Party = "lock"
	// PartyDownload: download endpoints were re-issued for the new path.
	PartyDownload Party = "download"
	// PartyUpload: upload endpoints were re-issued for the new path.
	PartyUpload Party = "upload"
)

// AllParties are the parties whose confirmations make a generation confirmed.
var AllParties = []Party{PartySessions, PartyAppProvider, PartyLock, PartyDownload, PartyUpload}

// Generation is one editing generation for a resource identity.
type Generation struct {
	Sequence      int64
	Resource      ResourceID
	OldPath       string
	NewPath       string
	State         GenState
	Confirmations map[Party]bool
	// SupersededBy is set on the previous (active) generation when a move
	// starts; 0 means it is still the unchallenged serving generation.
	SupersededBy int64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	CommittedAt  *time.Time
}

// Confirmed reports whether a party already confirmed.
func (g *Generation) Confirmed(p Party) bool {
	return g.Confirmations[p]
}

// AllConfirmed reports whether every party confirmed.
func (g *Generation) AllConfirmed() bool {
	for _, p := range AllParties {
		if !g.Confirmations[p] {
			return false
		}
	}
	return true
}

// SessionState is the lifecycle state of an editing session.
type SessionState string

const (
	// SessionActive: the resource is open for editing in the app.
	SessionActive SessionState = "active"
	// SessionClosed: the app closed the document.
	SessionClosed SessionState = "closed"
)

// Session is one read-write editing session.
type Session struct {
	ID          string
	Resource    ResourceID
	Generation  int64
	User        *userpb.UserId
	DisplayName string
	// AppProvider is the app provider serving the session (app name).
	AppProvider string
	// LockID is the storage write lock correlated with the session.
	LockID string
	// DownloadID / UploadID identify the endpoints issued for this session.
	DownloadID string
	UploadID   string
	State      SessionState
	OpenedAt   time.Time
	ClosedAt   *time.Time
}

// IsOpen reports whether the session is not closed.
func (s *Session) IsOpen() bool {
	return s.State != SessionClosed
}

// UploadState is the state of an in-flight save (upload session).
type UploadState string

const (
	// UploadOpen: the upload was initiated and the save is in flight.
	UploadOpen UploadState = "open"
	// UploadCompleted: the save completed.
	UploadCompleted UploadState = "completed"
	// UploadAborted: the save was aborted.
	UploadAborted UploadState = "aborted"
)

// InFlightUpload is a save registered at InitiateFileUpload, bound to the
// generation current at the time. The drain phase waits for open uploads.
type InFlightUpload struct {
	ID         string
	Resource   ResourceID
	Generation int64
	SessionID  string
	State      UploadState
	StartedAt  time.Time
	EndedAt    *time.Time
}

// CallbackKind is the kind of an app callback.
type CallbackKind string

const (
	// CallbackSaved: the app saved the document.
	CallbackSaved CallbackKind = "saved"
	// CallbackClosed: the app closed the document.
	CallbackClosed CallbackKind = "closed"
)

// Callback is an event the application reports back. Seq is the per-session
// sequence used for idempotency.
type Callback struct {
	SessionID  string
	Resource   ResourceID
	Generation int64
	Kind       CallbackKind
	Seq        int64
}
