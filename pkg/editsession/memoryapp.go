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
	"fmt"
	"sync"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// MemoryAppProvider is the in-repo reference implementation of the
// AppProvider port: it is the stand-in for the external wopiserver. It tracks
// the sessions currently open in the app, their takeover state and their last
// saved generation, and can be scripted to fail or delay for tests.
type MemoryAppProvider struct {
	mu sync.Mutex

	// sessions open in the app: session ID -> resource identity.
	sessions map[string]ResourceID
	// takenOver: session ID -> generation it was taken over to.
	takenOver map[string]int64
	// savedGen: session ID -> generation of the last recorded save.
	savedGen map[string]int64

	// TakeoverErr, when non-nil, makes TakeOver fail with it.
	TakeoverErr error
	// ConfirmErr, when non-nil, makes ConfirmReference fail with it.
	ConfirmErr error

	// confirmedRefs: generation -> true once the provider confirmed it.
	confirmedRefs map[int64]bool
}

// NewMemoryAppProvider returns a new MemoryAppProvider.
func NewMemoryAppProvider() *MemoryAppProvider {
	return &MemoryAppProvider{
		sessions:      map[string]ResourceID{},
		takenOver:     map[string]int64{},
		savedGen:      map[string]int64{},
		confirmedRefs: map[int64]bool{},
	}
}

// OpenSession records that a session is open in the app.
func (p *MemoryAppProvider) OpenSession(sessionID string, r ResourceID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[sessionID] = r
}

// RecordSave records a save for the session at the given generation. This is
// what an inbound app "saved" callback drives in a wired deployment.
func (p *MemoryAppProvider) RecordSave(sessionID string, generation int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.sessions[sessionID]; !ok {
		return fmt.Errorf("editsession: unknown session %s", sessionID)
	}
	p.savedGen[sessionID] = generation
	return nil
}

// TakeOver implements AppProvider. It is idempotent: a repeated takeover at
// the same generation is a no-op; taking over to an older generation fails.
func (p *MemoryAppProvider) TakeOver(_ context.Context, sessionID string, r ResourceID, newRef *provider.Reference, generation int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if res, ok := p.sessions[sessionID]; !ok {
		return fmt.Errorf("editsession: app has no session %s", sessionID)
	} else if res != r {
		return fmt.Errorf("editsession: session %s bound to another resource", sessionID)
	}
	if newRef == nil {
		return fmt.Errorf("editsession: takeover of %s missing new reference", sessionID)
	}
	if gen, ok := p.takenOver[sessionID]; ok {
		if gen == generation {
			return nil // already taken over to this generation
		}
		if gen > generation {
			return fmt.Errorf("editsession: session %s already at newer generation %d", sessionID, gen)
		}
	}
	if p.TakeoverErr != nil {
		return p.TakeoverErr
	}
	p.takenOver[sessionID] = generation
	return nil
}

// ConfirmReference implements AppProvider. Idempotent per generation.
func (p *MemoryAppProvider) ConfirmReference(_ context.Context, r ResourceID, newRef *provider.Reference, generation int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newRef == nil {
		return fmt.Errorf("editsession: confirmation of generation %d missing reference", generation)
	}
	if p.confirmedRefs[generation] {
		return nil
	}
	if p.ConfirmErr != nil {
		return p.ConfirmErr
	}
	p.confirmedRefs[generation] = true
	return nil
}

// ConfirmedReference reports whether the provider confirmed a generation.
func (p *MemoryAppProvider) ConfirmedReference(generation int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.confirmedRefs[generation]
}

// QuerySaved implements AppProvider.
func (p *MemoryAppProvider) QuerySaved(_ context.Context, sessionID string, r ResourceID) (bool, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	res, ok := p.sessions[sessionID]
	if !ok {
		return false, 0, fmt.Errorf("editsession: app has no session %s", sessionID)
	}
	if res != r {
		return false, 0, fmt.Errorf("editsession: session %s bound to another resource", sessionID)
	}
	gen, saved := p.savedGen[sessionID]
	return saved, gen, nil
}

// TakenOver reports whether and to which generation a session was taken over.
func (p *MemoryAppProvider) TakenOver(sessionID string) (generation int64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	generation, ok = p.takenOver[sessionID]
	return
}

// Reset clears all state.
func (p *MemoryAppProvider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions = map[string]ResourceID{}
	p.takenOver = map[string]int64{}
	p.savedGen = map[string]int64{}
	p.confirmedRefs = map[int64]bool{}
}
