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

package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	sp "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
	sqlstore "github.com/cs3org/reva/v3/pkg/editsession/store/sql"
	revaservice "github.com/cs3org/reva/v3/pkg/service"
	"google.golang.org/grpc"
)

const (
	gwStorageID = "storage"
	gwSpaceID   = "space"
	gwOpaqueID  = "inode-1"
	gwOldPath   = "/docs/file.docx"
	gwNewPath   = "/archive/file.docx"
	gwLockID    = "lock-1"
)

// richStorageProvider is a fake lock-capable storage provider with a tiny
// namespace (path -> opaque id) and per-path write locks.
type richStorageProvider struct {
	sp.ProviderAPIClient

	mu      sync.Mutex
	entries map[string]string // path -> opaque id
	locks   map[string]*sp.Lock
}

func newRichStorage() *richStorageProvider {
	return &richStorageProvider{
		entries: map[string]string{gwOldPath: gwOpaqueID},
		locks:   map[string]*sp.Lock{gwOldPath: {LockId: gwLockID}},
	}
}

// waitUntil polls cond until it is true or a deadline passes.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached in time")
}

func (s *richStorageProvider) Stat(_ context.Context, req *sp.StatRequest, _ ...grpc.CallOption) (*sp.StatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.entries[req.Ref.GetPath()]
	if !ok {
		return &sp.StatResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	return &sp.StatResponse{
		Status: &rpc.Status{Code: rpc.Code_CODE_OK},
		Info: &sp.ResourceInfo{
			Id:   &sp.ResourceId{StorageId: gwStorageID, SpaceId: gwSpaceID, OpaqueId: id},
			Path: req.Ref.GetPath(),
			Type: sp.ResourceType_RESOURCE_TYPE_FILE,
		},
	}, nil
}

func (s *richStorageProvider) GetLock(_ context.Context, req *sp.GetLockRequest, _ ...grpc.CallOption) (*sp.GetLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, ok := s.locks[req.Ref.GetPath()]
	if !ok {
		return &sp.GetLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	return &sp.GetLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}, Lock: lock}, nil
}

func (s *richStorageProvider) SetLock(_ context.Context, req *sp.SetLockRequest, _ ...grpc.CallOption) (*sp.SetLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.locks[req.Ref.GetPath()]; ok {
		return &sp.SetLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_FAILED_PRECONDITION, Message: "locked"}}, nil
	}
	s.locks[req.Ref.GetPath()] = req.Lock
	return &sp.SetLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}}, nil
}

func (s *richStorageProvider) RefreshLock(_ context.Context, req *sp.RefreshLockRequest, _ ...grpc.CallOption) (*sp.RefreshLockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lock, ok := s.locks[req.Ref.GetPath()]
	if !ok {
		return &sp.RefreshLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	if lock.GetLockId() != req.ExistingLockId {
		return &sp.RefreshLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_FAILED_PRECONDITION}}, nil
	}
	s.locks[req.Ref.GetPath()] = req.Lock
	return &sp.RefreshLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}}, nil
}

func (s *richStorageProvider) Unlock(_ context.Context, req *sp.UnlockRequest, _ ...grpc.CallOption) (*sp.UnlockResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.locks[req.Ref.GetPath()]; !ok {
		return &sp.UnlockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	delete(s.locks, req.Ref.GetPath())
	return &sp.UnlockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}}, nil
}

func (s *richStorageProvider) Move(_ context.Context, req *sp.MoveRequest, _ ...grpc.CallOption) (*sp.MoveResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, ok := s.entries[req.Source.GetPath()]
	if !ok {
		if _, atDest := s.entries[req.Destination.GetPath()]; atDest {
			return &sp.MoveResponse{Status: &rpc.Status{Code: rpc.Code_CODE_ALREADY_EXISTS}}, nil
		}
		return &sp.MoveResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	delete(s.entries, req.Source.GetPath())
	s.entries[req.Destination.GetPath()] = id
	return &sp.MoveResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}}, nil
}

func (s *richStorageProvider) InitiateFileDownload(_ context.Context, req *sp.InitiateFileDownloadRequest, _ ...grpc.CallOption) (*sp.InitiateFileDownloadResponse, error) {
	return &sp.InitiateFileDownloadResponse{
		Status: &rpc.Status{Code: rpc.Code_CODE_OK},
		Protocols: []*sp.FileDownloadProtocol{{
			Protocol:         "simple",
			DownloadEndpoint: "http://data" + req.Ref.GetPath(),
			Expose:           true,
		}},
	}, nil
}

func (s *richStorageProvider) InitiateFileUpload(_ context.Context, req *sp.InitiateFileUploadRequest, _ ...grpc.CallOption) (*sp.InitiateFileUploadResponse, error) {
	return &sp.InitiateFileUploadResponse{
		Status: &rpc.Status{Code: rpc.Code_CODE_OK},
		Protocols: []*sp.FileUploadProtocol{{
			Protocol:       "simple",
			UploadEndpoint: "http://data" + req.Ref.GetPath(),
			Expose:         true,
		}},
	}, nil
}

// unsupportedLockStorage returns CODE_UNIMPLEMENTED for lock calls.
type unsupportedLockStorage struct {
	*richStorageProvider
}

func (s *unsupportedLockStorage) GetLock(context.Context, *sp.GetLockRequest, ...grpc.CallOption) (*sp.GetLockResponse, error) {
	return &sp.GetLockResponse{Status: &rpc.Status{Code: rpc.Code_CODE_UNIMPLEMENTED}}, nil
}

// --- handoff test server ---

func newHandoffServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/takeover", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/internal/confirm", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/internal/savestate", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"saved": false, "generation": 0})
	})
	return httptest.NewServer(mux)
}

// --- harness ---

type genMoveHarness struct {
	svc     *svc
	storage *richStorageProvider
	r       editsession.ResourceID
}

func newGenMoveHarness(t *testing.T, storageSrv *richStorageProvider) *genMoveHarness {
	t.Helper()

	if storageSrv == nil {
		storageSrv = newRichStorage()
	}
	endpoint := t.Name() + "/storage"
	revaservice.SetClientAt(endpoint, storageSrv)
	t.Cleanup(func() { revaservice.SetClientAt(endpoint, nil) })

	reg := &fakeStorageRegistryServer{storageEndpoint: endpoint}
	// ensure the process-global test resolver is installed, capture the
	// previously stamped peers and restore them after the test, so this
	// harness does not pollute state for later tests in the suite.
	globalTestResolverOnce.Do(func() { revaservice.SetGlobal(globalTestResolver) })
	globalTestResolver.mu.Lock()
	prevReg := globalTestResolver.registry
	prevShares := globalTestResolver.shares
	globalTestResolver.registry = reg
	globalTestResolver.shares = nil
	globalTestResolver.mu.Unlock()
	t.Cleanup(func() {
		globalTestResolver.mu.Lock()
		globalTestResolver.registry = prevReg
		globalTestResolver.shares = prevShares
		globalTestResolver.mu.Unlock()
	})

	appSrv := newHandoffServer(t)

	r := editsession.ResourceID{StorageID: gwStorageID, SpaceID: gwSpaceID, OpaqueID: gwOpaqueID}
	s := &svc{
		c: &config{
			TransferSharedSecret: "transfer-secret",
			TransferExpires:      3600,
			EditSession: editSessionConfig{
				AppHandoffURL: appSrv.URL, AppHandoffSecret: "secret",
			},
		},
	}
	store := newSQLiteStore(t)
	app := newHTTPAppProvider(appSrv.URL, "secret")
	s.coord = editsession.New(store,
		svcStorage{s}, svcLocker{s}, app, svcEndpoints{s},
		editsession.Config{DrainPoll: 1000000})

	return &genMoveHarness{svc: s, storage: storageSrv, r: r}
}

func (h *genMoveHarness) registerSession(t *testing.T, id string) {
	t.Helper()
	sess := &editsession.Session{
		ID: id, Resource: h.r,
		User:   &userpb.UserId{OpaqueId: "jdoe", Idp: "example.org"},
		LockID: gwLockID,
	}
	if err := h.svc.coord.OpenSession(context.Background(), sess); err != nil {
		t.Fatalf("register session: %v", err)
	}
}

func moveReq(path, dest string) *sp.MoveRequest {
	return &sp.MoveRequest{
		Source:      &sp.Reference{Path: path},
		Destination: &sp.Reference{Path: dest},
	}
}

func TestGatewayMoveWithSessionConfirms(t *testing.T) {
	h := newGenMoveHarness(t, nil)
	ctx := context.Background()
	h.registerSession(t, "s1")

	res, err := h.svc.Move(ctx, moveReq(gwOldPath, gwNewPath))
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if res.Status.Code != rpc.Code_CODE_OK {
		t.Fatalf("expected ok, got %s: %s", res.Status.Code, res.Status.Message)
	}

	g, err := h.svc.coord.Store().GetGeneration(ctx, h.r, 2)
	if err != nil {
		t.Fatalf("get generation: %v", err)
	}
	if g.State != editsession.GenConfirmed || !g.AllConfirmed() {
		t.Fatalf("generation not confirmed: %+v", g)
	}
	// lock only at the new reference; entry moved; session relocated.
	if _, ok := h.storage.locks[gwNewPath]; !ok {
		t.Fatal("expected lock at new reference")
	}
	if _, ok := h.storage.locks[gwOldPath]; ok {
		t.Fatal("old reference lock must be released")
	}
	if _, ok := h.storage.entries[gwNewPath]; !ok {
		t.Fatal("entry must be at the new path")
	}
	sessions, _ := h.svc.coord.Store().ListSessions(ctx, h.r, 2, true)
	if len(sessions) != 1 || sessions[0].Generation != 2 {
		t.Fatalf("session not relocated: %+v", sessions)
	}
}

func TestGatewayMoveWithoutSessionsIsLegacy(t *testing.T) {
	h := newGenMoveHarness(t, nil)
	ctx := context.Background()

	res, err := h.svc.Move(ctx, moveReq(gwOldPath, gwNewPath))
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if res.Status.Code != rpc.Code_CODE_OK {
		t.Fatalf("expected legacy move ok, got %s", res.Status.Code)
	}
	// entry moved through the legacy path; no generation was created.
	if _, ok := h.storage.entries[gwNewPath]; !ok {
		t.Fatal("legacy move must relocate the entry")
	}
	gens, _ := h.svc.coord.Store().ListGenerations(ctx, h.r)
	if len(gens) != 0 {
		t.Fatalf("no generation may be created without sessions, got %d", len(gens))
	}
}

func TestGatewayMoveLocksUnsupportedIsLegacy(t *testing.T) {
	unsupported := &unsupportedLockStorage{richStorageProvider: newRichStorage()}
	h := newGenMoveHarness(t, unsupported.richStorageProvider)
	// session row exists but the probe says locks are unsupported.
	h.registerSession(t, "s1")

	res, err := h.svc.Move(context.Background(), moveReq(gwOldPath, gwNewPath))
	if err != nil {
		t.Fatalf("move: %v", err)
	}
	if res.Status.Code != rpc.Code_CODE_OK {
		t.Fatalf("expected legacy ok, got %s", res.Status.Code)
	}
	if _, ok := unsupported.entries[gwNewPath]; !ok {
		t.Fatal("legacy move must have relocated the entry")
	}
}

func TestGatewayFreezeBlocksOpenAndDrains(t *testing.T) {
	h := newGenMoveHarness(t, nil)
	ctx := context.Background()
	h.registerSession(t, "s1")

	// in-flight save at the serving generation.
	up := &editsession.InFlightUpload{ID: "up-1", Resource: h.r}
	if err := h.svc.coord.BeginUpload(ctx, up); err != nil {
		t.Fatalf("begin upload: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var moveStatus *rpc.Status
	go func() {
		defer wg.Done()
		res, err := h.svc.Move(ctx, moveReq(gwOldPath, gwNewPath))
		if err == nil {
			moveStatus = res.Status
		}
	}()

	waitUntil(t, func() bool {
		return errors.Is(h.svc.coord.CheckEditOpen(ctx, h.r), editsession.ErrFrozen)
	})

	// upload initiation while frozen is rejected as retriable.
	upRes, err := h.svc.InitiateFileUpload(ctx, &sp.InitiateFileUploadRequest{Ref: &sp.Reference{Path: gwOldPath}})
	if err != nil {
		t.Fatalf("initiate upload: %v", err)
	}
	if upRes.Status.Code != rpc.Code_CODE_FAILED_PRECONDITION {
		t.Fatalf("expected frozen/precondition, got %s", upRes.Status.Code)
	}

	if err := h.svc.coord.CompleteUpload(ctx, "up-1"); err != nil {
		t.Fatalf("complete: %v", err)
	}
	wg.Wait()
	if moveStatus == nil || moveStatus.Code != rpc.Code_CODE_OK {
		t.Fatal("move must confirm after drain")
	}
}

func TestGatewayUnlockGenerationGate(t *testing.T) {
	h := newGenMoveHarness(t, nil)
	ctx := context.Background()
	h.registerSession(t, "s1")

	if _, err := h.svc.Move(ctx, moveReq(gwOldPath, gwNewPath)); err != nil {
		t.Fatalf("move: %v", err)
	}

	// a late unlock carrying the resource id but the old path is rejected;
	// the lock remains.
	oldRef := &sp.Reference{Path: gwOldPath, ResourceId: h.r.ToCS3()}
	res, err := h.svc.Unlock(ctx, &sp.UnlockRequest{Ref: oldRef, Lock: &sp.Lock{LockId: gwLockID}})
	if err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if res.Status.Code != rpc.Code_CODE_FAILED_PRECONDITION {
		t.Fatalf("expected precondition rejection, got %s", res.Status.Code)
	}
	if _, ok := h.storage.locks[gwNewPath]; !ok {
		t.Fatal("new lock must survive the rejected unlock")
	}

	// current-generation unlock succeeds.
	res, err = h.svc.Unlock(ctx, &sp.UnlockRequest{Ref: &sp.Reference{Path: gwNewPath}, Lock: &sp.Lock{LockId: gwLockID}})
	if err != nil || res.Status.Code != rpc.Code_CODE_OK {
		t.Fatalf("current unlock: %v %+v", err, res)
	}
	if _, ok := h.storage.locks[gwNewPath]; ok {
		t.Fatal("lock must be released")
	}
}

// helpers shared with the gateway test package.
func newSQLiteStore(t *testing.T) editsession.Store {
	t.Helper()
	st, err := sqlstore.New(context.Background(), map[string]any{
		"db_engine": "sqlite",
		"db_name":   filepath.Join(t.TempDir(), "gateway-editsession.db"),
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	return st
}
