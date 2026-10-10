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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	sp "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
	sqlstore "github.com/cs3org/reva/v3/pkg/editsession/store/sql"
	"github.com/cs3org/reva/v3/pkg/errtypes"

	"errors"
)

// editSessionConfig is the gateway's `editsession` configuration section.
type editSessionConfig struct {
	sqlstore.Config `mapstructure:",squash"`
	Enabled         bool `mapstructure:"enabled"`
	// AppHandoffURL is the base URL of the HTTP appprovider internal
	// endpoints; when empty the generation protocol is not activated.
	AppHandoffURL string `mapstructure:"app_handoff_url"`
	// AppHandoffSecret authenticates server-to-server handoff calls.
	AppHandoffSecret string `mapstructure:"app_handoff_secret"`
}

// resourceOf resolves the resource identity of a reference: directly from its
// ResourceId when complete, otherwise through a Stat on the resolved
// provider. Returns ok=false when it cannot be resolved.
func (s *svc) resourceOf(ctx context.Context, ref *sp.Reference) (editsession.ResourceID, bool) {
	if rid := ref.GetResourceId(); rid.GetStorageId() != "" && rid.GetOpaqueId() != "" {
		if r, err := editsession.ResourceIDFromCS3(rid); err == nil {
			return r, true
		}
	}
	c, err := s.find(ctx, ref)
	if err != nil {
		return editsession.ResourceID{}, false
	}
	res, err := c.Stat(ctx, &sp.StatRequest{Ref: ref})
	if err != nil || res.GetStatus().GetCode() != rpc.Code_CODE_OK || res.GetInfo() == nil {
		return editsession.ResourceID{}, false
	}
	r, err := editsession.ResourceIDFromCS3(res.GetInfo().GetId())
	return r, err == nil
}

// errToStatus maps an error to a CS3 status for gateway responses.
func errToStatus(err error) *rpc.Status {
	var (
		pd errtypes.IsPermissionDenied
		nf errtypes.IsNotFound
		br errtypes.IsBadRequest
		cf interface{ IsConflict() }
		ns errtypes.IsNotSupported
	)
	switch {
	case errors.As(err, &pd):
		return &rpc.Status{Code: rpc.Code_CODE_PERMISSION_DENIED, Message: err.Error()}
	case errors.As(err, &nf):
		return &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND, Message: err.Error()}
	case errors.As(err, &br):
		return &rpc.Status{Code: rpc.Code_CODE_FAILED_PRECONDITION, Message: err.Error()}
	case errors.As(err, &cf):
		// frozen generation: a retriable precondition for the caller.
		return &rpc.Status{Code: rpc.Code_CODE_FAILED_PRECONDITION, Message: err.Error()}
	case errors.As(err, &ns):
		return &rpc.Status{Code: rpc.Code_CODE_UNIMPLEMENTED, Message: err.Error()}
	default:
		return &rpc.Status{Code: rpc.Code_CODE_INTERNAL, Message: err.Error()}
	}
}

// genMoveIfEligible runs the generation protocol when the source resource is
// lock-capable, has open editing sessions and a handoff endpoint is
// configured. handled=false tells the caller to take the legacy move path.
// Probe errors never block the request: they fall back to legacy.
func (s *svc) genMoveIfEligible(ctx context.Context, client sp.ProviderAPIClient, req *sp.MoveRequest) (*sp.MoveResponse, bool, error) {
	if s.coord == nil {
		return nil, false, nil
	}

	statRes, err := client.Stat(ctx, &sp.StatRequest{Ref: req.Source})
	if err != nil || statRes.GetStatus().GetCode() != rpc.Code_CODE_OK || statRes.GetInfo() == nil {
		return nil, false, nil
	}
	rid, err := editsession.ResourceIDFromCS3(statRes.GetInfo().GetId())
	if err != nil {
		return nil, false, nil
	}

	// lock capability probe: OK/NOT_FOUND mean locks are supported.
	lockRes, err := client.GetLock(ctx, &sp.GetLockRequest{Ref: req.Source})
	if err != nil {
		return nil, false, nil
	}
	switch lockRes.GetStatus().GetCode() {
	case rpc.Code_CODE_OK, rpc.Code_CODE_NOT_FOUND:
	default:
		return nil, false, nil
	}

	n, err := s.coord.Store().CountOpenSessions(ctx, rid, 0)
	if err != nil {
		return nil, false, nil
	}
	if n == 0 {
		return nil, false, nil
	}
	if s.c.EditSession.AppHandoffURL == "" {
		return nil, false, nil
	}

	if _, err := s.coord.Move(ctx, rid, req.Source, req.Destination); err != nil {
		// session counts disagreed (e.g. after a rollback): do not block the
		// move on an internal error, take the legacy path.
		if errors.Is(err, editsession.ErrNoSessions) {
			return nil, false, nil
		}
		return &sp.MoveResponse{Status: errToStatus(err)}, true, nil
	}
	return &sp.MoveResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}}, true, nil
}

// statusToError maps a non-OK CS3 status to the matching errtypes error.
func statusToError(st *rpc.Status) error {
	switch st.GetCode() {
	case rpc.Code_CODE_OK:
		return nil
	case rpc.Code_CODE_NOT_FOUND:
		return errtypes.NotFound(st.GetMessage())
	case rpc.Code_CODE_PERMISSION_DENIED:
		return errtypes.PermissionDenied(st.GetMessage())
	case rpc.Code_CODE_FAILED_PRECONDITION:
		return errtypes.BadRequest(st.GetMessage())
	case rpc.Code_CODE_UNIMPLEMENTED:
		return errtypes.NotSupported(st.GetMessage())
	default:
		return errtypes.InternalError(st.GetMessage())
	}
}

// svcStorage adapts the gateway service to editsession.Storage. The provider
// client is resolved per call from the reference, so one coordinator serves
// every provider without rebuilding.
type svcStorage struct{ s *svc }

func (a svcStorage) client(ctx context.Context, ref *sp.Reference) (sp.ProviderAPIClient, error) {
	return a.s.find(ctx, ref)
}

// Move implements editsession.Storage.
func (a svcStorage) Move(ctx context.Context, oldRef, newRef *sp.Reference) error {
	c, err := a.client(ctx, oldRef)
	if err != nil {
		return err
	}
	res, err := c.Move(ctx, &sp.MoveRequest{Source: oldRef, Destination: newRef})
	if err != nil {
		return err
	}
	return statusToError(res.Status)
}

// Stat implements editsession.Storage.
func (a svcStorage) Stat(ctx context.Context, ref *sp.Reference) (*sp.ResourceInfo, error) {
	c, err := a.client(ctx, ref)
	if err != nil {
		return nil, err
	}
	res, err := c.Stat(ctx, &sp.StatRequest{Ref: ref})
	if err != nil {
		return nil, err
	}
	if err := statusToError(res.Status); err != nil {
		return nil, err
	}
	return res.Info, nil
}

// svcLocker adapts the gateway to editsession.Locker.
type svcLocker struct{ s *svc }

func (a svcLocker) GetLock(ctx context.Context, ref *sp.Reference) (*sp.Lock, error) {
	c, err := a.s.find(ctx, ref)
	if err != nil {
		return nil, err
	}
	res, err := c.GetLock(ctx, &sp.GetLockRequest{Ref: ref})
	if err != nil {
		return nil, err
	}
	if err := statusToError(res.Status); err != nil {
		return nil, err
	}
	return res.Lock, nil
}

func (a svcLocker) SetLock(ctx context.Context, ref *sp.Reference, lock *sp.Lock) error {
	c, err := a.s.find(ctx, ref)
	if err != nil {
		return err
	}
	res, err := c.SetLock(ctx, &sp.SetLockRequest{Ref: ref, Lock: lock})
	if err != nil {
		return err
	}
	return statusToError(res.Status)
}

func (a svcLocker) RefreshLock(ctx context.Context, ref *sp.Reference, lock *sp.Lock, existingLockID string) error {
	c, err := a.s.find(ctx, ref)
	if err != nil {
		return err
	}
	res, err := c.RefreshLock(ctx, &sp.RefreshLockRequest{Ref: ref, Lock: lock, ExistingLockId: existingLockID})
	if err != nil {
		return err
	}
	return statusToError(res.Status)
}

func (a svcLocker) Unlock(ctx context.Context, ref *sp.Reference, lock *sp.Lock) error {
	c, err := a.s.find(ctx, ref)
	if err != nil {
		return err
	}
	res, err := c.Unlock(ctx, &sp.UnlockRequest{Ref: ref, Lock: lock})
	if err != nil {
		return err
	}
	return statusToError(res.Status)
}

// svcEndpoints re-issues endpoints through the gateway's own initiate methods.
type svcEndpoints struct{ s *svc }

// ReissueDownload re-runs the download initiation for the new path. The
// endpoint identity is the re-signed transfer token.
func (e svcEndpoints) ReissueDownload(ctx context.Context, _ *editsession.Session, ref *sp.Reference) (string, error) {
	res, err := e.s.InitiateFileDownload(ctx, &sp.InitiateFileDownloadRequest{Ref: ref})
	if err != nil {
		return "", err
	}
	if err := statusToError(res.Status); err != nil {
		return "", err
	}
	for _, p := range res.Protocols {
		if p.GetToken() != "" {
			return p.GetToken(), nil
		}
	}
	return "download:" + ref.GetPath(), nil
}

// reissueKey marks the context as an internal endpoint reissue so the
// InitiateFileUpload path does not recursively register an in-flight save.
type reissueKey struct{}

func withReissue(ctx context.Context) context.Context {
	return context.WithValue(ctx, reissueKey{}, true)
}

func isReissue(ctx context.Context) bool {
	v, _ := ctx.Value(reissueKey{}).(bool)
	return v
}

// ReissueUpload re-runs the upload initiation (without a new save
// registration) and returns its transfer token as endpoint identity.
func (e svcEndpoints) ReissueUpload(ctx context.Context, sess *editsession.Session, ref *sp.Reference) (string, error) {
	res, err := e.s.InitiateFileUpload(withReissue(ctx), &sp.InitiateFileUploadRequest{Ref: ref})
	if err != nil {
		return "", err
	}
	if err := statusToError(res.Status); err != nil {
		return "", err
	}
	for _, p := range res.Protocols {
		if p.GetToken() != "" {
			return p.GetToken(), nil
		}
	}
	return "upload:" + ref.GetPath(), nil
}

// --- HTTP app provider client ---

// httpAppProvider calls the internal takeover endpoints hosted by the HTTP
// appprovider service (the in-repo counterpart of the external wopiserver).
type httpAppProvider struct {
	baseURL string
	secret  string
	client  *http.Client
}

func newHTTPAppProvider(baseURL, secret string) *httpAppProvider {
	return &httpAppProvider{
		baseURL: baseURL,
		secret:  secret,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (h *httpAppProvider) do(ctx context.Context, method, path string, body any) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-Reva-Secret", h.secret)

	res, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusOK {
		return nil
	}
	msg, _ := io.ReadAll(res.Body)
	switch res.StatusCode {
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return fmt.Errorf("%w: %s", editsession.ErrAppCallbackTimeout, string(msg))
	case http.StatusNotFound:
		return errtypes.NotFound(string(msg))
	default:
		return errtypes.InternalError(fmt.Sprintf("app handoff: %s: %s", res.Status, string(msg)))
	}
}

// TakeOver implements editsession.AppProvider.
func (h *httpAppProvider) TakeOver(ctx context.Context, sessionID string, r editsession.ResourceID, newRef *sp.Reference, generation int64) error {
	return h.do(ctx, http.MethodPost, "/internal/takeover", map[string]any{
		"session_id": sessionID,
		"resource":   r.Key(),
		"new_path":   newRef.GetPath(),
		"generation": generation,
	})
}

// QuerySaved implements editsession.AppProvider.
func (h *httpAppProvider) QuerySaved(ctx context.Context, sessionID string, r editsession.ResourceID) (bool, int64, error) {
	path := fmt.Sprintf("/internal/savestate?session_id=%s&resource=%s", sessionID, r.Key())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.baseURL+path, nil)
	if err != nil {
		return false, 0, err
	}
	req.Header.Set("X-Reva-Secret", h.secret)
	res, err := h.client.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(res.Body)
		return false, 0, errtypes.InternalError(string(msg))
	}
	out := struct {
		Saved bool  `json:"saved"`
		Gen   int64 `json:"generation"`
	}{}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return false, 0, err
	}
	return out.Saved, out.Gen, nil
}

// ConfirmReference implements editsession.AppProvider.
func (h *httpAppProvider) ConfirmReference(ctx context.Context, r editsession.ResourceID, newRef *sp.Reference, generation int64) error {
	return h.do(ctx, http.MethodPost, "/internal/confirm", map[string]any{
		"resource":   r.Key(),
		"new_path":   newRef.GetPath(),
		"generation": generation,
	})
}
