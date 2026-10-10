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
	"strconv"
	"time"

	typespb "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/golang-jwt/jwt/v5"
)

// SessionTokenClaims are the claims of the token bound to an editing session.
// It is what an application presents when posting callbacks.
type SessionTokenClaims struct {
	jwt.RegisteredClaims
	// SessionID identifies the editing session.
	SessionID string `json:"sid"`
	// ResourceKey is the resource identity the session belongs to.
	ResourceKey string `json:"rid"`
	// Generation is the generation sequence the token was minted for.
	Generation int64 `json:"gen"`
}

// TokenSigner mints and verifies session tokens (HS256).
type TokenSigner struct {
	secret []byte
	ttl    time.Duration
}

// NewTokenSigner builds a signer. ttl bounds the session token lifetime
// (default 1 hour when zero).
func NewTokenSigner(secret string, ttl time.Duration) (*TokenSigner, error) {
	if secret == "" {
		return nil, fmt.Errorf("editsession: session token secret must not be empty")
	}
	if ttl == 0 {
		ttl = time.Hour
	}
	return &TokenSigner{secret: []byte(secret), ttl: ttl}, nil
}

// Mint signs a token for the session.
func (s *TokenSigner) Mint(sessionID, resourceKey string, generation int64, now time.Time) (string, error) {
	if now.IsZero() {
		now = time.Now()
	}
	claims := SessionTokenClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
			Audience:  jwt.ClaimStrings{"reva"},
		},
		SessionID:   sessionID,
		ResourceKey: resourceKey,
		Generation:  generation,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(s.secret)
	if err != nil {
		return "", fmt.Errorf("editsession: signing session token: %w", err)
	}
	return signed, nil
}

// Verify validates a session token and returns its claims.
func (s *TokenSigner) Verify(token string) (*SessionTokenClaims, error) {
	claims := &SessionTokenClaims{}
	parsed, err := jwt.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("editsession: unexpected signing method %s", t.Method.Alg())
		}
		return s.secret, nil
	})
	if err != nil {
		return nil, fmt.Errorf("editsession: verifying session token: %w", err)
	}
	if !parsed.Valid {
		return nil, fmt.Errorf("editsession: invalid session token")
	}
	return claims, nil
}

// --- transfer JWT extension ---

// JSON claim names added to the transfer JWT. Older data gateways simply
// ignore unknown claims, so extending the token stays backward compatible.
const (
	TransferClaimResource = "resource"
	TransferClaimGen      = "gen"
	TransferClaimUploadID = "upload_id"
)

// TransferExtension are the generation fields attached to a transfer token.
type TransferExtension struct {
	ResourceKey     string
	Generation      int64
	UploadSessionID string
}

// --- Opaque keys between gateway and appprovider ---

// Opaque keys used to hand the session identity from the gateway to the
// appprovider service (and its drivers) without changing the CS3 protocol.
const (
	OpaqueKeySessionID  = "editsession.session_id"
	OpaqueKeyGeneration = "editsession.generation"
	OpaqueKeyResource   = "editsession.resource"
)

// EncodeSessionOpaque builds an Opaque carrying the session identity.
func EncodeSessionOpaque(sessionID, resourceKey string, generation int64) *typespb.Opaque {
	return &typespb.Opaque{
		Map: map[string]*typespb.OpaqueEntry{
			OpaqueKeySessionID: {Decoder: "plain", Value: []byte(sessionID)},
			OpaqueKeyGeneration: {
				Decoder: "plain",
				Value:   []byte(fmt.Sprintf("%d", generation)),
			},
			OpaqueKeyResource: {Decoder: "plain", Value: []byte(resourceKey)},
		},
	}
}

// DecodeSessionOpaque extracts the session identity. ok is false when the
// opaque does not carry a session id.
func DecodeSessionOpaque(o *typespb.Opaque) (sessionID, resourceKey string, generation int64, ok bool) {
	if o == nil || o.Map == nil {
		return "", "", 0, false
	}
	sidEntry, has := o.Map[OpaqueKeySessionID]
	if !has {
		return "", "", 0, false
	}
	sessionID = string(sidEntry.Value)
	if e := o.Map[OpaqueKeyResource]; e != nil {
		resourceKey = string(e.Value)
	}
	if e := o.Map[OpaqueKeyGeneration]; e != nil {
		if gen, err := strconv.ParseInt(string(e.Value), 10, 64); err == nil {
			generation = gen
		}
	}
	return sessionID, resourceKey, generation, true
}

// CallbackDedupKey is the idempotency key of an app callback.
func CallbackDedupKey(sessionID string, seq int64) string {
	return fmt.Sprintf("%s#%d", sessionID, seq)
}
