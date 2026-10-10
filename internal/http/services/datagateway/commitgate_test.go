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

package datagateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cs3org/reva/v3/pkg/editsession"
	sqlstore "github.com/cs3org/reva/v3/pkg/editsession/store/sql"
	"github.com/golang-jwt/jwt/v5"
)

const gateSecret = "transfer-secret"

// gateBackend is the internal data server the gateway proxies to.
type gateBackend struct {
	mu       sync.Mutex
	putCalls int
	status   int
}

func newGateBackend() *gateBackend {
	return &gateBackend{status: http.StatusOK}
}

func (b *gateBackend) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.mu.Lock()
		b.putCalls++
		status := b.status
		b.mu.Unlock()
		w.WriteHeader(status)
	})
}

func (b *gateBackend) puts() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.putCalls
}

// gateFixture bundles the service, its backing store (second connection on
// the same database, used to seed state) and the backend.
type gateFixture struct {
	svc        http.Handler
	store      editsession.Store
	backend    *gateBackend
	backendURL string
}

func newGateFixture(t *testing.T, enabled bool) *gateFixture {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "datagateway.db")
	backend := newGateBackend()
	backendServer := httptest.NewServer(backend.handler())
	t.Cleanup(backendServer.Close)

	m := map[string]any{
		"prefix":                 "datagateway",
		"transfer_shared_secret": gateSecret,
		"timeout":                30,
	}
	if enabled {
		m["commit_check"] = true
		m["db_engine"] = "sqlite"
		m["db_name"] = dbPath
	}
	s, err := New(context.Background(), m)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}

	// second store connection to seed generation/session/upload state.
	st, err := sqlstore.New(context.Background(), map[string]any{
		"db_engine": "sqlite",
		"db_name":   dbPath,
	})
	if err != nil {
		t.Fatalf("seed store: %v", err)
	}

	return &gateFixture{svc: s.Handler(), store: st, backend: backend, backendURL: backendServer.URL}
}

// seedActiveSession creates the base generation and an open session.
func (f *gateFixture) seedActiveSession(t *testing.T, r editsession.ResourceID) {
	t.Helper()
	coord := editsession.New(f.store, nil, nil, nil, nil)
	sess := &editsession.Session{ID: "s1", Resource: r, LockID: "lock-1"}
	if err := coord.OpenSession(context.Background(), sess); err != nil {
		t.Fatalf("open session: %v", err)
	}
}

// seedInFlightUpload registers an open upload bound to the serving gen.
func (f *gateFixture) seedInFlightUpload(t *testing.T, r editsession.ResourceID, id string) {
	t.Helper()
	coord := editsession.New(f.store, nil, nil, nil, nil)
	up := &editsession.InFlightUpload{ID: id, Resource: r}
	if err := coord.BeginUpload(context.Background(), up); err != nil {
		t.Fatalf("begin upload: %v", err)
	}
}

// seedConfirmedSuccessor creates G2 confirmed (new path) and marks G1
// superseded, plus an open G1 upload used to exercise stale saves.
func (f *gateFixture) seedConfirmedSuccessor(t *testing.T, r editsession.ResourceID, uploadID string) {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	g2 := &editsession.Generation{
		Sequence: 2, Resource: r,
		OldPath: "/docs/file.docx", NewPath: "/archive/file.docx",
		State: editsession.GenConfirmed,
		Confirmations: map[editsession.Party]bool{
			editsession.PartySessions: true, editsession.PartyAppProvider: true,
			editsession.PartyLock:     true,
			editsession.PartyDownload: true, editsession.PartyUpload: true,
		},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := f.store.CreateGeneration(ctx, g2); err != nil {
		t.Fatalf("create g2: %v", err)
	}
	if err := f.store.MarkSuperseded(ctx, r, 1, 2); err != nil {
		t.Fatalf("mark superseded: %v", err)
	}
	// stale upload bound to G1.
	if err := f.store.RegisterUpload(ctx, &editsession.InFlightUpload{
		ID: uploadID, Resource: r, Generation: 1,
		State: editsession.UploadOpen, StartedAt: now,
	}); err != nil {
		t.Fatalf("register stale upload: %v", err)
	}
}

// mintTransferToken signs a transfer JWT the way the gateway does.
func mintTransferToken(target, uploadID, resource string, gen int64) string {
	claims := transferClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
		Target: target, UploadID: uploadID, Resource: resource, Gen: gen,
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(gateSecret))
	if err != nil {
		panic(err)
	}
	return token
}

func putThrough(t *testing.T, h http.Handler, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/datagateway/put", strings.NewReader("data"))
	req.Header.Set(TokenTransportHeader, token)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestPutWithCurrentGenerationProxies(t *testing.T) {
	f := newGateFixture(t, true)
	r := editsession.ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "inode"}
	f.seedActiveSession(t, r)
	f.seedInFlightUpload(t, r, "up-current")

	token := mintTransferToken(f.backendURL, "up-current", r.Key(), 1)
	rec := putThrough(t, f.svc, token)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected current-gen put to proxy with 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if f.backend.puts() != 1 {
		t.Fatal("the internal data server must have received the PUT")
	}
}

func TestPutWithOldGenerationRejected(t *testing.T) {
	f := newGateFixture(t, true)
	r := editsession.ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "inode"}
	f.seedActiveSession(t, r)
	f.seedConfirmedSuccessor(t, r, "up-old")

	token := mintTransferToken(f.backendURL, "up-old", r.Key(), 1)
	rec := putThrough(t, f.svc, token)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected stale save rejected with 409, got %d", rec.Code)
	}
	if f.backend.puts() != 0 {
		t.Fatal("the rejected PUT must never reach the data server")
	}
}

func TestPutWithoutSessionClaimProxies(t *testing.T) {
	f := newGateFixture(t, true)
	// no session/upload state at all; token has no upload_id: legacy path.
	token := mintTransferToken(f.backendURL, "", "", 0)
	rec := putThrough(t, f.svc, token)

	if rec.Code != http.StatusOK {
		t.Fatalf("plain upload must proxy regardless, got %d", rec.Code)
	}
	if f.backend.puts() != 1 {
		t.Fatal("the data server must have received the plain PUT")
	}
}

func TestPutWithoutGateEnabledProxies(t *testing.T) {
	f := newGateFixture(t, false)
	token := mintTransferToken(f.backendURL, "up-anything", "r", 5)
	rec := putThrough(t, f.svc, token)

	if rec.Code != http.StatusOK {
		t.Fatalf("with the gate disabled every upload proxies, got %d", rec.Code)
	}
	if f.backend.puts() != 1 {
		t.Fatal("the data server must receive the PUT")
	}
}
