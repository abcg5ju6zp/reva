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
)

// Store is the persistent store of generations, sessions and in-flight
// uploads. Every state transition is written synchronously, so a crash at any
// phase leaves enough information to resume the generation from that phase.
type Store interface {
	// --- generations ---

	// CreateGeneration inserts a new generation. It fails if a generation
	// with the same sequence already exists for the resource.
	CreateGeneration(ctx context.Context, g *Generation) error
	// GetGeneration returns one generation.
	GetGeneration(ctx context.Context, r ResourceID, seq int64) (*Generation, error)
	// GetInProgressGeneration returns the generation in freezing/moving
	// state for the resource, or nil with no error if there is none.
	GetInProgressGeneration(ctx context.Context, r ResourceID) (*Generation, error)
	// ListGenerations returns all generations of the resource, ordered by
	// sequence ascending.
	ListGenerations(ctx context.Context, r ResourceID) ([]*Generation, error)
	// TransitionGeneration moves a generation to a new state, provided its
	// persisted state is one of the `from` states. The returned generation
	// carries the new state.
	TransitionGeneration(ctx context.Context, r ResourceID, seq int64, from []GenState, to GenState) (*Generation, error)
	// MarkSuperseded records that the generation was succeeded by `by`.
	MarkSuperseded(ctx context.Context, r ResourceID, seq, by int64) error
	// ClearSuperseded removes the successor marker (used on rollback).
	ClearSuperseded(ctx context.Context, r ResourceID, seq int64) error
	// ClearSupersededIf removes the marker only while it points at
	// expectedSuperseder (no-op otherwise), so recovery cannot clear a
	// marker belonging to a different generation.
	ClearSupersededIf(ctx context.Context, r ResourceID, seq, expectedSuperseder int64) error
	// RecordConfirmation records one party's confirmation. It fails if the
	// generation is not in moving state.
	RecordConfirmation(ctx context.Context, r ResourceID, seq int64, party Party) error
	// ListRecoverableGenerations returns every generation in
	// freezing/moving state across all resources.
	ListRecoverableGenerations(ctx context.Context) ([]*Generation, error)
	// ListAbortedGenerations returns generations left in aborted state (a
	// crash during the abort rollback), so recovery can finish rebinding.
	ListAbortedGenerations(ctx context.Context) ([]*Generation, error)
	// DeleteGeneration removes the generation.
	DeleteGeneration(ctx context.Context, r ResourceID, seq int64) error

	// --- sessions ---

	// CreateSession inserts a new session.
	CreateSession(ctx context.Context, s *Session) error
	// GetSession returns one session.
	GetSession(ctx context.Context, id string) (*Session, error)
	// ListSessions returns the sessions of the resource. When genSeq > 0
	// they are restricted to that generation. When openOnly is set, closed
	// sessions are excluded.
	ListSessions(ctx context.Context, r ResourceID, genSeq int64, openOnly bool) ([]*Session, error)
	// CountOpenSessions counts open sessions of the resource, restricted
	// to the generation when genSeq > 0.
	CountOpenSessions(ctx context.Context, r ResourceID, genSeq int64) (int, error)
	// CloseSession marks a session closed. It is idempotent.
	CloseSession(ctx context.Context, id string, at time.Time) error
	// SaveSession persists all mutable fields of a session (generation and
	// re-issued endpoints) after takeover.
	SaveSession(ctx context.Context, s *Session) error
	// RecordCallback records an app callback (session, seq). It returns true
	// when the callback is new, false when an identical callback was already
	// applied, so the caller can enforce idempotency.
	RecordCallback(ctx context.Context, r ResourceID, sessionID string, seq int64, kind CallbackKind) (isNew bool, err error)

	// --- in-flight uploads ---

	// RegisterUpload inserts a new in-flight upload.
	RegisterUpload(ctx context.Context, u *InFlightUpload) error
	// GetUpload returns one upload.
	GetUpload(ctx context.Context, id string) (*InFlightUpload, error)
	// CompleteUpload marks an upload completed. Idempotent.
	CompleteUpload(ctx context.Context, id string, at time.Time) error
	// AbortUpload marks an upload aborted. Idempotent.
	AbortUpload(ctx context.Context, id string, at time.Time) error
	// ListOpenUploads returns open uploads of the resource, restricted to
	// the generation when genSeq > 0.
	ListOpenUploads(ctx context.Context, r ResourceID, genSeq int64) ([]*InFlightUpload, error)
}
