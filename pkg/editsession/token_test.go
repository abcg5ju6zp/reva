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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestSessionTokenMintVerify(t *testing.T) {
	signer, err := NewTokenSigner("a-secret", time.Hour)
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	now := time.Now()
	token, err := signer.Mint("s1", "stor!sp!inode", 2, now)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	claims, err := signer.Verify(token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.SessionID != "s1" || claims.ResourceKey != "stor!sp!inode" || claims.Generation != 2 {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestSessionTokenRejects(t *testing.T) {
	signer, _ := NewTokenSigner("secret-1", time.Hour)
	other, _ := NewTokenSigner("secret-2", time.Hour)

	t.Run("wrong secret", func(t *testing.T) {
		token, _ := signer.Mint("s1", "r", 1, time.Now())
		if _, err := other.Verify(token); err == nil {
			t.Fatal("expected verification failure with a different secret")
		}
	})
	t.Run("tampered body", func(t *testing.T) {
		token, _ := signer.Mint("s1", "r", 1, time.Now())
		parts := strings.Split(token, ".")
		parts[1] = parts[1] + "x"
		if _, err := signer.Verify(strings.Join(parts, ".")); err == nil {
			t.Fatal("expected tampered token rejected")
		}
	})
	t.Run("expired", func(t *testing.T) {
		past := time.Now().Add(-2 * time.Hour)
		token, _ := signer.Mint("s1", "r", 1, past)
		if _, err := signer.Verify(token); err == nil {
			t.Fatal("expected expired token rejected")
		}
	})
	t.Run("none algorithm rejected", func(t *testing.T) {
		claims := SessionTokenClaims{SessionID: "s1"}
		token, _ := jwt.NewWithClaims(jwt.SigningMethodNone, claims).SignedString(jwt.UnsafeAllowNoneSignatureType)
		if _, err := signer.Verify(token); err == nil {
			t.Fatal("expected none-algorithm token rejected")
		}
	})
}

func TestEmptySecretRejected(t *testing.T) {
	if _, err := NewTokenSigner("", 0); err == nil {
		t.Fatal("expected error for empty secret")
	}
}

// legacyTransferClaims is the shape an older data gateway understands: only
// target and version_key. Unknown fields must not break its verification.
type legacyTransferClaims struct {
	jwt.RegisteredClaims
	Target     string `json:"target"`
	VersionKey string `json:"version_key,omitempty"`
}

// extendedTransferClaims mirrors what the wired gateway signs after this
// change: legacy fields plus the generation extension.
type extendedTransferClaims struct {
	jwt.RegisteredClaims
	Target     string `json:"target"`
	VersionKey string `json:"version_key,omitempty"`

	Resource string `json:"resource,omitempty"`
	Gen      int64  `json:"gen,omitempty"`
	UploadID string `json:"upload_id,omitempty"`
}

func TestTransferClaimsBackwardCompatible(t *testing.T) {
	secret := []byte("transfer-secret")

	// a new-style token (with extension) must verify as a legacy token.
	newClaims := extendedTransferClaims{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		Target:           "http://data/internal/path",
		Resource:         "stor!sp!inode",
		Gen:              2,
		UploadID:         "up-1",
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, newClaims).SignedString(secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	legacy := &legacyTransferClaims{}
	parsed, err := jwt.ParseWithClaims(signed, legacy, func(t *jwt.Token) (any, error) { return secret, nil })
	if err != nil {
		t.Fatalf("legacy verify of extended token: %v", err)
	}
	if !parsed.Valid || legacy.Target != "http://data/internal/path" {
		t.Fatalf("legacy claims not restored: %+v", legacy)
	}

	// an old-style token must also parse with the extended struct.
	oldClaims := legacyTransferClaims{
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
		Target:           "http://data/old",
	}
	oldSigned, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, oldClaims).SignedString(secret)
	ext := &extendedTransferClaims{}
	if _, err := jwt.ParseWithClaims(oldSigned, ext, func(t *jwt.Token) (any, error) { return secret, nil }); err != nil {
		t.Fatalf("extended verify of legacy token: %v", err)
	}
	if ext.Target != "http://data/old" || ext.Gen != 0 || ext.Resource != "" {
		t.Fatalf("unexpected extended parse of legacy token: %+v", ext)
	}
}

func TestSessionOpaqueRoundTrip(t *testing.T) {
	o := EncodeSessionOpaque("s1", "stor!sp!inode", 2)
	sid, resource, gen, ok := DecodeSessionOpaque(o)
	if !ok || sid != "s1" || resource != "stor!sp!inode" || gen != 2 {
		t.Fatalf("opaque round trip failed: %s %s %d %v", sid, resource, gen, ok)
	}
	if _, _, _, ok := DecodeSessionOpaque(nil); ok {
		t.Fatal("nil opaque must not decode")
	}
	// opaque without a session id fails.
	noSession := EncodeSessionOpaque("", "", 0)
	delete(noSession.Map, OpaqueKeySessionID)
	if _, _, _, ok := DecodeSessionOpaque(noSession); ok {
		t.Fatal("opaque without session id must not decode")
	}
}

func TestCallbackDedupKey(t *testing.T) {
	if CallbackDedupKey("s1", 1) == CallbackDedupKey("s1", 2) {
		t.Fatal("different sequence must yield different dedup keys")
	}
	if CallbackDedupKey("s1", 1) != CallbackDedupKey("s1", 1) {
		t.Fatal("same callback must yield a stable dedup key")
	}
}

func TestParseInt(t *testing.T) {
	cases := map[string]int64{
		"1": 1, "42": 42, "-7": -7, "0": 0,
	}
	for in, want := range cases {
		got, err := strconv.ParseInt(in, 10, 64)
		if err != nil || got != want {
			t.Fatalf("ParseInt(%s) = %d, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "-", "1a"} {
		if _, err := strconv.ParseInt(bad, 10, 64); err == nil {
			t.Fatalf("ParseInt(%q) should fail", bad)
		}
	}
}
