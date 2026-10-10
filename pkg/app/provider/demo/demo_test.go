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

package demo

import (
	"context"
	"strings"
	"testing"

	appprovider "github.com/cs3org/go-cs3apis/cs3/app/provider/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/editsession"
)

func resourceWithSession() *provider.ResourceInfo {
	r := editsession.ResourceID{StorageID: "stor", SpaceID: "sp", OpaqueID: "inode"}
	return &provider.ResourceInfo{
		Id:     r.ToCS3(),
		Opaque: editsession.EncodeSessionOpaque("s1", r.Key(), 2),
	}
}

func TestGetAppURLEmbedsSession(t *testing.T) {
	p := &demoProvider{iframeUIProvider: "http://app"}

	res, _, err := p.GetAppURL(context.Background(), resourceWithSession(),
		appprovider.ViewMode_VIEW_MODE_READ_WRITE, "access-token", "en")
	if err != nil {
		t.Fatalf("get app url: %v", err)
	}
	if !strings.Contains(res.AppUrl, "session-id=s1") || !strings.Contains(res.AppUrl, "generation=2") {
		t.Fatalf("session identity not embedded: %s", res.AppUrl)
	}
}

func TestGetAppURLWithoutSession(t *testing.T) {
	p := &demoProvider{iframeUIProvider: "http://app"}
	r := editsession.ResourceID{StorageID: "stor", OpaqueID: "inode"}
	res, _, err := p.GetAppURL(context.Background(),
		&provider.ResourceInfo{Id: r.ToCS3()},
		appprovider.ViewMode_VIEW_MODE_READ_ONLY, "tok", "en")
	if err != nil {
		t.Fatalf("get app url: %v", err)
	}
	if strings.Contains(res.AppUrl, "session-id=") {
		t.Fatalf("no session params expected: %s", res.AppUrl)
	}
	if !strings.HasSuffix(strings.TrimSpace(res.AppUrl), "/>") {
		t.Fatalf("malformed url: %s", res.AppUrl)
	}
}
