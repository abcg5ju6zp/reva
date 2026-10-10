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
	"strconv"
	"sync"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
)

// EndpointCall records one reissue call.
type EndpointCall struct {
	Download bool
	Session  string
	Ref      *provider.Reference
}

// FakeEndpoints is a scriptable EndpointIssuer. It returns deterministic
// endpoint ids ("dl-<n>" / "ul-<n>") and records the calls.
type FakeEndpoints struct {
	mu sync.Mutex

	// DownloadErr / UploadErr, when non-nil, fail the corresponding reissue.
	DownloadErr error
	UploadErr   error

	counter int
	calls   []EndpointCall
}

// ReissueDownload implements EndpointIssuer.
func (f *FakeEndpoints) ReissueDownload(_ context.Context, s *Session, ref *provider.Reference) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, EndpointCall{Download: true, Session: s.ID, Ref: ref})
	if f.DownloadErr != nil {
		return "", f.DownloadErr
	}
	f.counter++
	return endpointID("dl", f.counter), nil
}

// ReissueUpload implements EndpointIssuer.
func (f *FakeEndpoints) ReissueUpload(_ context.Context, s *Session, ref *provider.Reference) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, EndpointCall{Download: false, Session: s.ID, Ref: ref})
	if f.UploadErr != nil {
		return "", f.UploadErr
	}
	f.counter++
	return endpointID("ul", f.counter), nil
}

func endpointID(prefix string, n int) string {
	return prefix + "-" + strconv.Itoa(n)
}

// Calls returns the recorded calls.
func (f *FakeEndpoints) Calls() []EndpointCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]EndpointCall, len(f.calls))
	copy(out, f.calls)
	return out
}
