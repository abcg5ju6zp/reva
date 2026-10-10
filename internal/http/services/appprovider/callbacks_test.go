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
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cs3org/reva/v3/pkg/editsession"
)

const callbackSecret = "internal-secret"

func newCallbackServer(t *testing.T) (*httptest.Server, *editsession.TokenSigner) {
	t.Helper()
	s, err := New(context.Background(), map[string]any{
		"prefix":          "app",
		"gatewaysvc":      "http://localhost:0",
		"internal_secret": callbackSecret,
	})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	server := httptest.NewServer(s.Handler())
	t.Cleanup(server.Close)

	signer, err := editsession.NewTokenSigner(callbackSecret, time.Hour)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	return server, signer
}

func sessionToken(t *testing.T, signer *editsession.TokenSigner, sid, resource string, gen int64) string {
	t.Helper()
	token, err := signer.Mint(sid, resource, gen, time.Now())
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	return token
}

func TestAppCallbackSaved(t *testing.T) {
	server, signer := newCallbackServer(t)
	token := sessionToken(t, signer, "s1", "stor!sp!inode", 2)

	post := func(auth string) int {
		req, _ := http.NewRequest(http.MethodPost, server.URL+"/callback", strings.NewReader(`{"kind":"saved"}`))
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", "Bearer "+auth)
		}
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		defer res.Body.Close()
		return res.StatusCode
	}

	// missing and invalid session tokens are rejected.
	if code := post(""); code != http.StatusUnauthorized {
		t.Fatalf("missing token: code=%d, want 401", code)
	}
	if code := post("not-a-token"); code != http.StatusUnauthorized {
		t.Fatalf("invalid token: code=%d, want 401", code)
	}
	if code := post(token); code != http.StatusOK {
		t.Fatalf("valid saved callback: code=%d, want 200", code)
	}
	// repeated callback is idempotent.
	if code := post(token); code != http.StatusOK {
		t.Fatalf("repeated callback: code=%d, want 200", code)
	}

	// the save state is now observable via the internal endpoint.
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/internal/savestate?session_id=s1", nil)
	req.Header.Set(InternalSecretKey, callbackSecret)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("savestate: %v", err)
	}
	defer res.Body.Close()
	body := make([]byte, 128)
	n, _ := res.Body.Read(body)
	if !strings.Contains(string(body[:n]), `"saved":true`) {
		t.Fatalf("expected saved state, got %s", body[:n])
	}
}

func TestAppCallbackClosed(t *testing.T) {
	server, signer := newCallbackServer(t)
	token := sessionToken(t, signer, "s1", "r", 1)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/callback", strings.NewReader(`{"kind":"closed"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("closed callback: code=%d, want 200", res.StatusCode)
	}

	// an unknown kind is rejected.
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/callback", strings.NewReader(`{"kind":"nope"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	res, _ = server.Client().Do(req)
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown kind: code=%d, want 400", res.StatusCode)
	}
	res.Body.Close()
}

func TestInternalEndpointsAuthentication(t *testing.T) {
	server, _ := newCallbackServer(t)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/internal/takeover"},
		{http.MethodPost, "/internal/confirm"},
		{http.MethodGet, "/internal/savestate"},
	}
	for _, c := range cases {
		// no secret: unauthorized.
		req, _ := http.NewRequest(c.method, server.URL+c.path, strings.NewReader("{}"))
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s without secret: code=%d, want 401", c.path, res.StatusCode)
		}
		res.Body.Close()

		// wrong secret: unauthorized.
		req, _ = http.NewRequest(c.method, server.URL+c.path, strings.NewReader("{}"))
		req.Header.Set(InternalSecretKey, "wrong")
		res, _ = server.Client().Do(req)
		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("%s wrong secret: code=%d, want 401", c.path, res.StatusCode)
		}
		res.Body.Close()

		// correct secret: accepted.
		req, _ = http.NewRequest(c.method, server.URL+c.path, strings.NewReader("{}"))
		req.Header.Set(InternalSecretKey, callbackSecret)
		res, _ = server.Client().Do(req)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s correct secret: code=%d, want 200", c.path, res.StatusCode)
		}
		res.Body.Close()
	}
}
