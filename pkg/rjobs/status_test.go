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

package rjobs

import (
	"context"
	"testing"
	"time"
)

// TestCommitTerminalCarriesTimestamps guards against the terminal write
// blanking the start time recorded at adoption: the fenced commit must carry
// StartedAt forward and set FinishedAt.
func TestCommitTerminalCarriesTimestamps(t *testing.T) {
	status := newFakeStatus()
	ctx := context.Background()
	enqueued := time.Now().Add(-time.Minute).Truncate(time.Second)
	started := enqueued.Add(time.Second)

	if err := status.Put(ctx, Status{
		RunID: "r-startedat", Job: "test.startedat", State: StateQueued,
		Attempt: 1, EnqueuedAt: enqueued,
	}); err != nil {
		t.Fatal(err)
	}

	if _, _, err := status.Adopt(ctx, Status{
		RunID: "r-startedat", Job: "test.startedat", Attempt: 1, EnqueuedAt: enqueued,
	}, 1); err != nil {
		t.Fatal(err)
	}

	finished := time.Now()
	committed, err := status.CommitTerminal(ctx, Status{
		RunID: "r-startedat", Job: "test.startedat", State: StateSucceeded,
		Attempt: 1, EnqueuedAt: enqueued, StartedAt: &started, FinishedAt: &finished,
		Result: Params{"ok": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("the owning attempt should form the terminal state")
	}

	got, err := status.Get(ctx, "r-startedat")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.State != StateSucceeded {
		t.Fatalf("state = %q, want succeeded", got.State)
	}
	if got.StartedAt == nil || !got.StartedAt.Equal(started) {
		t.Fatalf("StartedAt = %v, want %v", got.StartedAt, started)
	}
	if got.FinishedAt == nil || !got.FinishedAt.Equal(finished) {
		t.Fatalf("FinishedAt = %v, want %v", got.FinishedAt, finished)
	}
	if got.Result["ok"] != true {
		t.Fatalf("result = %v, want ok=true", got.Result)
	}
}
