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

package appprovider

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/cs3org/reva/v3/pkg/editsession"
)

// InternalSecretKey is the header shared between gateway and this service for
// server-to-server calls.
const InternalSecretKey = "X-Reva-Secret"

// callbackState records app events reported for sessions. It backs the save
// state the gateway queries when reconciling an ambiguous takeover.
type callbackState struct {
	mu sync.Mutex
	// saved: sessionID -> generation last saved.
	saved map[string]int64
	// closed: sessionID -> true once closed.
	closed map[string]bool
}

func newCallbackState() *callbackState {
	return &callbackState{saved: map[string]int64{}, closed: map[string]bool{}}
}

func (c *callbackState) recordSaved(sessionID string, generation int64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen, ok := c.saved[sessionID]; ok && gen == generation {
		return false // already recorded
	}
	c.saved[sessionID] = generation
	return true
}

func (c *callbackState) recordClosed(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed[sessionID] = true
}

func (c *callbackState) savedGen(sessionID string) (int64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gen, ok := c.saved[sessionID]
	return gen, ok
}

// appCallbackRequest is the body of an application callback. The session
// identity (id, resource, generation) comes from the verified session token.
type appCallbackRequest struct {
	Kind string `json:"kind"`
}

// handleAppCallback accepts a save/close event from the application,
// authenticated by the session token the gateway minted at open time.
func (s *svc) handleAppCallback(w http.ResponseWriter, r *http.Request) {
	if s.verifier == nil {
		http.Error(w, "callbacks are not configured", http.StatusServiceUnavailable)
		return
	}
	token := bearerToken(r)
	if token == "" {
		http.Error(w, "missing session token", http.StatusUnauthorized)
		return
	}
	claims, err := s.verifier.Verify(token)
	if err != nil {
		http.Error(w, "invalid session token", http.StatusUnauthorized)
		return
	}

	req := appCallbackRequest{}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	switch editsession.CallbackKind(req.Kind) {
	case editsession.CallbackSaved:
		s.callbacks.recordSaved(claims.SessionID, claims.Generation)
	case editsession.CallbackClosed:
		s.callbacks.recordClosed(claims.SessionID)
	default:
		http.Error(w, "unknown callback kind", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// checkInternalSecret authenticates a gateway server-to-server call.
func (s *svc) checkInternalSecret(r *http.Request) bool {
	return s.conf.InternalSecret != "" && r.Header.Get(InternalSecretKey) == s.conf.InternalSecret
}

// handleInternalTakeOver is the gateway-facing takeover endpoint.
func (s *svc) handleInternalTakeOver(w http.ResponseWriter, r *http.Request) {
	if !s.checkInternalSecret(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleInternalConfirm is the gateway-facing provider confirmation.
func (s *svc) handleInternalConfirm(w http.ResponseWriter, r *http.Request) {
	if !s.checkInternalSecret(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleInternalSaveState reports the saved state the gateway reconciles
// against after a callback timeout.
func (s *svc) handleInternalSaveState(w http.ResponseWriter, r *http.Request) {
	if !s.checkInternalSecret(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	sessionID := r.URL.Query().Get("session_id")
	gen, saved := s.callbacks.savedGen(sessionID)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"saved": saved, "generation": gen,
	})
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) > len(prefix) && h[:len(prefix)] == prefix {
		return h[len(prefix):]
	}
	return ""
}
