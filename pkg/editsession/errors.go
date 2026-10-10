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
	"fmt"

	"github.com/cs3org/reva/v3/pkg/errtypes"
)

// Sentinel errors. They are wrapped with context at the call site (using %w),
// keep their type interfaces, and are distinguishable by callers from
// ordinary network/storage failures.
var (
	// ErrFrozen signals that a resource identity has an in-progress
	// generation: new edit opens are frozen and must retry / be redirected.
	ErrFrozen errtypes.Conflict = "editsession: resource is frozen for a generation move"
	// ErrGenerationMismatch signals an operation bound to a generation that
	// is not the current one: post-commit old-gen saves, old-gen unlocks and
	// stale callbacks fail with it.
	ErrGenerationMismatch errtypes.BadRequest = "editsession: operation does not match the current generation"
	// ErrAppCallbackTimeout signals that an app confirmation did not arrive
	// in time and must be reconciled by session and resource identity.
	ErrAppCallbackTimeout errtypes.InternalError = "editsession: app callback timed out"
	// ErrNoSessions signals that a generation move was attempted without an
	// active editing session; the caller must use the plain move path.
	ErrNoSessions errtypes.BadRequest = "editsession: no active editing sessions for a generation move"
	// ErrAlreadyMoved signals that the directory entry is already at the
	// destination (observed when resuming a generation).
	ErrAlreadyMoved errtypes.AlreadyExists = "editsession: directory entry already at destination"
)

// wrap returns a sentinel wrapped with context.
func wrap(err error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", err, fmt.Sprintf(format, args...))
}
