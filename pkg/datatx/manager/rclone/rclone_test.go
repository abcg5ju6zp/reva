// Copyright 2018-2024 CERN
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

package rclone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	datatx "github.com/cs3org/go-cs3apis/cs3/tx/v1beta1"

	txdriver "github.com/cs3org/reva/v3/pkg/datatx"
	"github.com/cs3org/reva/v3/pkg/datatx/manager/rclone/repository"

	// registers the json repository used by the driver
	_ "github.com/cs3org/reva/v3/pkg/datatx/manager/rclone/repository/json"
)

const (
	srcPath = "/source"
	dstPath = "/dest"
	srcURI  = "https://srctoken@src.example.com?name=/source"
	dstURI  = "https://dsttoken@dst.example.com?name=/dest"
)

// ---- mock rclone daemon ----

type mockJob struct {
	id        int64
	startTime string
	polls     int
	finished  bool
	success   bool
	err       string
	stopped   bool
}

type mockRclone struct {
	srv *httptest.Server

	mu sync.Mutex

	copyCount   int
	stopCount   int
	jobs        map[int64]*mockJob
	nextJobID   int64
	listIsFile  bool
	copyMode    string // "ok" | "reject" | "lost"
	pollsToDone int    // number of status polls while running before the job finishes; <= 0 means never
	finishError string
	stopError   string
	onCopy      func()
}

func newMockRclone() *mockRclone {
	m := &mockRclone{
		jobs:     map[int64]*mockJob{},
		copyMode: "ok",
	}
	m.srv = httptest.NewServer(http.HandlerFunc(m.handle))
	return m
}

func (m *mockRclone) close() { m.srv.Close() }

func (m *mockRclone) handle(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/operations/list":
		m.handleList(w)
	case "/sync/copy":
		m.handleCopy(w, r)
	case "/job/status":
		m.handleStatus(w, r)
	case "/job/stop":
		m.handleStop(w, r)
	case "/job/list":
		m.handleJobList(w)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (m *mockRclone) handleList(w http.ResponseWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.listIsFile {
		writeJSON(w, http.StatusOK, map[string]any{
			"list": []map[string]any{
				{"Path": srcPath, "Name": "source", "IsDir": false},
			},
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"list": []map[string]any{
			{"Path": srcPath, "Name": "source", "IsDir": true},
			{"Path": srcPath + "/a", "Name": "a", "IsDir": false},
		},
	})
}

func (m *mockRclone) handleCopy(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.copyCount++
	if m.onCopy != nil {
		m.onCopy()
	}

	switch m.copyMode {
	case "reject":
		m.mu.Unlock()
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":  "explicit rejection",
			"status": http.StatusInternalServerError,
		})
		return
	case "lost":
		// rclone accepts and creates the job, but the response never arrives
		m.nextJobID++
		id := m.nextJobID
		m.jobs[id] = &mockJob{id: id, startTime: time.Now().Format(time.RFC3339Nano)}
		m.mu.Unlock()

		hj, ok := w.(http.Hijacker)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		conn, _, err := hj.Hijack()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = conn.Close()
		return
	}

	m.nextJobID++
	id := m.nextJobID
	m.jobs[id] = &mockJob{id: id, startTime: time.Now().Format(time.RFC3339Nano)}
	m.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"jobid": id})
}

func (m *mockRclone) jobStatus(j *mockJob) map[string]any {
	endTime := ""
	if j.finished {
		endTime = time.Now().Format(time.RFC3339Nano)
	}
	return map[string]any{
		"id":        j.id,
		"finished":  j.finished,
		"success":   j.success,
		"error":     j.err,
		"startTime": j.startTime,
		"endTime":   endTime,
		"duration":  0,
		"group":     fmt.Sprintf("job/%d", j.id),
	}
}

func (m *mockRclone) handleStatus(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID int64 `json:"jobid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[req.JobID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":  "job not found",
			"status": http.StatusNotFound,
		})
		return
	}

	j.polls++
	if !j.finished && m.pollsToDone > 0 && j.polls >= m.pollsToDone {
		j.finished = true
		j.success = m.finishError == ""
		j.err = m.finishError
	}

	writeJSON(w, http.StatusOK, m.jobStatus(j))
}

func (m *mockRclone) handleStop(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JobID int64 `json:"jobid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	j, ok := m.jobs[req.JobID]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error":  "job not found",
			"status": http.StatusNotFound,
		})
		return
	}

	m.stopCount++
	j.finished = true
	j.stopped = true
	if m.stopError != "" {
		j.success = false
		j.err = m.stopError
	} else {
		j.success = true
		j.err = ""
	}
	writeJSON(w, http.StatusOK, m.jobStatus(j))
}

func (m *mockRclone) handleJobList(w http.ResponseWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make([]int64, 0, len(m.jobs))
	for id := range m.jobs {
		ids = append(ids, id)
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobids": ids})
}

func (m *mockRclone) counts() (copyCount, stopCount int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.copyCount, m.stopCount
}

// ---- helpers ----

func newTestDriver(t *testing.T, m *mockRclone, jobFile string, timeoutMs int) (*rclone, txdriver.Manager) {
	t.Helper()
	conf := map[string]any{
		"endpoint":                   m.srv.URL,
		"auth_user":                  "admin",
		"auth_pass":                  "",
		"job_status_check_interval":  5,
		"job_timeout":                timeoutMs,
		"storagedriver":              "json",
		"storagedrivers": map[string]any{
			"json": map[string]any{"file": jobFile},
		},
	}
	mgr, err := New(context.Background(), conf)
	if err != nil {
		t.Fatalf("error creating driver: %v", err)
	}
	return mgr.(*rclone), mgr
}

func jobFilePath(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/jobs.json"
}

func eventually(timeout time.Duration, f func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if f() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return f()
}

func statusOf(d txdriver.Manager, txID string) datatx.Status {
	info, err := d.GetTransferStatus(context.Background(), txID)
	if err != nil {
		return datatx.Status_STATUS_INVALID
	}
	return info.Status
}

func stopAllMonitors(d *rclone) {
	d.monMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(d.monitors))
	for _, c := range d.monitors {
		cancels = append(cancels, c)
	}
	d.monMu.Unlock()
	for _, c := range cancels {
		c()
	}
}

// ---- tests ----

// Normal success, then an identical request is adopted: no second job.
func TestCreateSuccessAndIdempotentRetry(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.pollsToDone = 2

	_, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Status != datatx.Status_STATUS_TRANSFER_NEW {
		t.Fatalf("unexpected initial status: %v", info.Status)
	}
	txID := info.Id.OpaqueId

	if !eventually(3*time.Second, func() bool {
		return statusOf(mgr, txID) == datatx.Status_STATUS_TRANSFER_COMPLETE
	}) {
		t.Fatalf("transfer did not complete, status: %v", statusOf(mgr, txID))
	}

	// same request again: same identity, adopted, no new remote job
	info2, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error on retry: %v", err)
	}
	if info2.Id.OpaqueId != txID {
		t.Fatalf("retry created a different transfer id: %v != %v", info2.Id.OpaqueId, txID)
	}
	copyCount, _ := mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected exactly 1 remote copy, got %d", copyCount)
	}
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_COMPLETE {
		t.Fatalf("unexpected status: %v", got)
	}
}

// Single file paths remain rejected with INVALID, and never start a copy.
func TestSingleFileRejected(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.listIsFile = true

	_, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err == nil {
		t.Fatal("expected error for single file transfer")
	}
	if info.Status != datatx.Status_STATUS_INVALID {
		t.Fatalf("unexpected status: %v", info.Status)
	}
	copyCount, _ := mock.counts()
	if copyCount != 0 {
		t.Fatalf("expected no copy for single file, got %d", copyCount)
	}

	// retrying does not start a copy either
	if _, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI); err == nil {
		t.Fatal("expected error on retry for single file")
	}
	copyCount, _ = mock.counts()
	if copyCount != 0 {
		t.Fatalf("expected no copy on retry, got %d", copyCount)
	}
}

// Explicit remote rejection persists FAILED; retries adopt the record
// instead of creating another job. A subsequent RetryTransfer starts a new
// generation that completes.
func TestExplicitRejectionThenRetry(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.copyMode = "reject"

	d, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err == nil {
		t.Fatal("expected rejection error")
	}
	if info.Status != datatx.Status_STATUS_TRANSFER_FAILED {
		t.Fatalf("unexpected status: %v", info.Status)
	}
	txID := info.Id.OpaqueId
	copyCount, _ := mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected 1 copy attempt, got %d", copyCount)
	}

	// same request retried: adoption of final state, no new copy
	if _, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI); err == nil {
		t.Fatal("expected error adopting failed transfer")
	}
	copyCount, _ = mock.counts()
	if copyCount != 1 {
		t.Fatalf("retry after rejection must not start another copy, got %d", copyCount)
	}

	// now an explicit retry: new generation succeeds
	mock.copyMode = "ok"
	mock.pollsToDone = 1
	_, err = mgr.RetryTransfer(context.Background(), txID)
	if err != nil {
		t.Fatalf("unexpected retry error: %v", err)
	}
	if !eventually(3*time.Second, func() bool {
		return statusOf(mgr, txID) == datatx.Status_STATUS_TRANSFER_COMPLETE
	}) {
		t.Fatalf("retried transfer did not complete: %v", statusOf(mgr, txID))
	}
	copyCount, _ = mock.counts()
	if copyCount != 2 {
		t.Fatalf("expected 2 copies after retry, got %d", copyCount)
	}
	stored, err := d.storage.GetJob(txID)
	if err != nil {
		t.Fatalf("error getting stored job: %v", err)
	}
	if stored.Generation != 2 {
		t.Fatalf("expected generation 2 after retry, got %d", stored.Generation)
	}
}

// rclone accepted the job but the local mapping could not be published:
// the unconfirmed remote job is cancelled and a failure record retained.
// A later identical request converges to the existing job, never copying twice.
func TestMappingPublishFailureCancelsJob(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	jobFile := jobFilePath(t)
	jobDir := filepath.Dir(jobFile)

	_, mgr := newTestDriver(t, mock, jobFile, 60000)

	mock.onCopy = func() {
		// break persistence of the mapping (and of the failure record):
		// a read-only directory blocks temp-file creation and rename
		_ = os.Chmod(jobDir, 0555)
	}

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	// restore before asserting so cleanup always runs on a writable dir
	_ = os.Chmod(jobDir, 0755)
	if err == nil {
		t.Fatal("expected creation error")
	}
	if info.Status != datatx.Status_STATUS_TRANSFER_FAILED {
		t.Fatalf("unexpected status: %v", info.Status)
	}
	txID := info.Id.OpaqueId

	copyCount, stopCount := mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected 1 copy, got %d", copyCount)
	}
	if stopCount != 1 {
		t.Fatalf("expected the unconfirmed job to be stopped, stops: %d", stopCount)
	}

	// retry the same request: pending record is recovered by identity and
	// converges to the stopped job instead of copying again
	info2, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error on recovery: %v", err)
	}
	if info2.Id.OpaqueId != txID {
		t.Fatalf("recovery created a different transfer: %v", info2.Id.OpaqueId)
	}
	copyCount, _ = mock.counts()
	if copyCount != 1 {
		t.Fatalf("no second copy allowed after publish failure, got %d", copyCount)
	}
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_COMPLETE {
		t.Fatalf("expected recovered complete state, got %v", got)
	}
}

// Remote start response lost: no mapping exists, so no monitor runs. The
// retried request queries the daemon by identity, claims the job and lets
// it complete - exactly one copy.
func TestLostStartResponseRecovery(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.copyMode = "lost"
	mock.pollsToDone = 4

	d, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err == nil {
		t.Fatal("expected error due lost response")
	}
	txID := info.Id.OpaqueId
	copyCount, _ := mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected 1 copy, got %d", copyCount)
	}

	// retry after a short delay: identity-based recovery claims the running job
	time.Sleep(20 * time.Millisecond)
	info2, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error recovering lost response: %v", err)
	}
	if info2.Id.OpaqueId != txID {
		t.Fatalf("recovery created a different transfer: %v", info2.Id.OpaqueId)
	}

	if !eventfullyCompletes(mgr, txID) {
		t.Fatalf("recovered transfer did not complete: %v", statusOf(mgr, txID))
	}
	copyCount, _ = mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected exactly 1 copy, got %d", copyCount)
	}
	stored, err := d.storage.GetJob(txID)
	if err != nil {
		t.Fatalf("error getting stored job: %v", err)
	}
	if stored.Generation != 1 {
		t.Fatalf("recovery must not bump generation, got %d", stored.Generation)
	}
}

func eventfullyCompletes(mgr txdriver.Manager, txID string) bool {
	return eventually(3*time.Second, func() bool {
		return statusOf(mgr, txID) == datatx.Status_STATUS_TRANSFER_COMPLETE
	})
}

// Cancel while completion could interleave: within one generation the first
// final state wins and stays unique; the monitor's late complete is discarded.
func TestCancelWinsUniqueFinalState(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.pollsToDone = 0 // never completes by itself

	_, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := info.Id.OpaqueId

	if !eventually(3*time.Second, func() bool {
		return statusOf(mgr, txID) == datatx.Status_STATUS_TRANSFER_IN_PROGRESS
	}) {
		t.Fatalf("transfer did not start: %v", statusOf(mgr, txID))
	}

	cancelInfo, err := mgr.CancelTransfer(context.Background(), txID)
	if err != nil {
		t.Fatalf("unexpected cancel error: %v", err)
	}
	if cancelInfo.Status != datatx.Status_STATUS_TRANSFER_CANCELLED {
		t.Fatalf("unexpected cancel status: %v", cancelInfo.Status)
	}

	// wait long enough for the same-generation monitor to poll at least once
	// more; its late COMPLETE must not overwrite CANCELLED
	time.Sleep(50 * time.Millisecond)
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_CANCELLED {
		t.Fatalf("unique final state violated, got %v", got)
	}

	// cancelling again reports the existing end state without another stop
	if _, err := mgr.CancelTransfer(context.Background(), txID); err == nil {
		t.Fatal("expected error cancelling an already final job")
	}
	_, stopCount := mock.counts()
	if stopCount != 1 {
		t.Fatalf("expected exactly 1 stop, got %d", stopCount)
	}
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_CANCELLED {
		t.Fatalf("status changed unexpectedly: %v", got)
	}
}

// When completion wins, a later cancel cannot overwrite it nor stop the job.
func TestCompleteWinsOverCancel(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.pollsToDone = 1

	_, mgr := newTestDriver(t, mock, jobFilePath(t), 60000)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := info.Id.OpaqueId

	if !eventfullyCompletes(mgr, txID) {
		t.Fatalf("transfer did not complete: %v", statusOf(mgr, txID))
	}

	cancelInfo, err := mgr.CancelTransfer(context.Background(), txID)
	if err == nil {
		t.Fatal("expected error cancelling a completed transfer")
	}
	if cancelInfo.Status != datatx.Status_STATUS_INVALID {
		t.Fatalf("unexpected cancel status: %v", cancelInfo.Status)
	}
	_, stopCount := mock.counts()
	if stopCount != 0 {
		t.Fatalf("completed job must not be stopped, got %d stops", stopCount)
	}
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_COMPLETE {
		t.Fatalf("complete state was overwritten: %v", got)
	}
}

// After timeout the job expires; RetryTransfer publishes a new generation.
// A late status transition of the old remote job is rejected by the
// generation guard and cannot overwrite the new generation.
func TestExpiredThenRetryGenerationGuard(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.pollsToDone = 0 // never finishes

	d, mgr := newTestDriver(t, mock, jobFilePath(t), 60)

	info, err := mgr.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := info.Id.OpaqueId

	if !eventually(3*time.Second, func() bool {
		return statusOf(mgr, txID) == datatx.Status_STATUS_TRANSFER_EXPIRED
	}) {
		t.Fatalf("job did not expire: %v", statusOf(mgr, txID))
	}

	// retry: a new generation is started
	mock.pollsToDone = 1
	retryInfo, err := mgr.RetryTransfer(context.Background(), txID)
	if err != nil {
		t.Fatalf("unexpected retry error: %v", err)
	}
	if retryInfo.Status != datatx.Status_STATUS_TRANSFER_NEW {
		t.Fatalf("unexpected retry status: %v", retryInfo.Status)
	}

	if !eventfullyCompletes(mgr, txID) {
		t.Fatalf("new generation did not complete: %v", statusOf(mgr, txID))
	}

	stored, err := d.storage.GetJob(txID)
	if err != nil {
		t.Fatalf("error getting stored job: %v", err)
	}
	if stored.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", stored.Generation)
	}

	// a late final status of the old remote job is rejected, not stored
	_, err = d.storage.TransitionStatus(
		&repository.Job{TransferID: txID, Generation: 1},
		datatx.Status_STATUS_TRANSFER_COMPLETE)
	if err != repository.ErrGenerationConflict {
		t.Fatalf("expected generation conflict, got: %v", err)
	}
	if got := statusOf(mgr, txID); got != datatx.Status_STATUS_TRANSFER_COMPLETE {
		t.Fatalf("final state was affected by old generation: %v", got)
	}
}

// Service restart: a new driver instance over the same storage takes over
// the running job; no new copy is started.
func TestRestartTakesOverRunningJob(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()
	mock.pollsToDone = 25

	file := jobFilePath(t)
	d1, mgr1 := newTestDriver(t, mock, file, 60000)

	info, err := mgr1.CreateTransfer(context.Background(), srcURI, dstURI)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txID := info.Id.OpaqueId

	if !eventually(3*time.Second, func() bool {
		return statusOf(mgr1, txID) == datatx.Status_STATUS_TRANSFER_IN_PROGRESS
	}) {
		t.Fatalf("job did not start: %v", statusOf(mgr1, txID))
	}

	// simulate process death
	stopAllMonitors(d1)
	copyCount, _ := mock.counts()
	if copyCount != 1 {
		t.Fatalf("expected 1 copy before restart, got %d", copyCount)
	}

	// restart: new instance takes over the stored non-final job
	d2, mgr2 := newTestDriver(t, mock, file, 60000)

	stored, err := d2.storage.GetJob(txID)
	if err != nil {
		t.Fatalf("error getting job after restart: %v", err)
	}
	if stored.Generation != 1 || stored.JobID < 0 {
		t.Fatalf("restart did not take over original job: %+v", stored)
	}

	if !eventfullyCompletes(mgr2, txID) {
		t.Fatalf("taken-over job did not complete: %v", statusOf(mgr2, txID))
	}
	copyCount, _ = mock.counts()
	if copyCount != 1 {
		t.Fatalf("restart must not start another copy, got %d", copyCount)
	}
}

// Existing authentication header handling is preserved for both schemes.
func TestBuildFsAuthHeaders(t *testing.T) {
	mock := newMockRclone()
	defer mock.close()

	d, _ := newTestDriver(t, mock, jobFilePath(t), 60000)

	job := &repository.Job{
		SrcToken:   "src-tok",
		SrcRemote:  "https://src.example.com",
		SrcPath:    srcPath,
		DestToken:  "dst-tok",
		DestRemote: "https://dst-tok@dst.example.com",
		DestPath:   dstPath,
	}

	srcFs, dstFs := d.buildFs(job)
	if !strings.Contains(srcFs, `bearer_token="src-tok"`) || !strings.Contains(dstFs, `bearer_token="dst-tok"`) {
		t.Fatalf("expected bearer headers, got: %s | %s", srcFs, dstFs)
	}

	d.config.AuthHeader = "x-access-token"
	srcFs, dstFs = d.buildFs(job)
	if !strings.Contains(srcFs, `headers="x-access-token,src-tok"`) || !strings.Contains(dstFs, `headers="x-access-token,dst-tok"`) {
		t.Fatalf("expected x-access-token headers, got: %s | %s", srcFs, dstFs)
	}
}
