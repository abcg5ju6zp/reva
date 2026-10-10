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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	userv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	datatx "github.com/cs3org/go-cs3apis/cs3/tx/v1beta1"
	typespb "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/cs3org/reva/v3/pkg/appctx"
	txdriver "github.com/cs3org/reva/v3/pkg/datatx"
	"github.com/cs3org/reva/v3/pkg/datatx/manager/rclone/repository"
	repoRegistry "github.com/cs3org/reva/v3/pkg/datatx/manager/rclone/repository/registry"
	registry "github.com/cs3org/reva/v3/pkg/datatx/manager/registry"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/httpclient"
)

func init() {
	registry.Register("rclone", New)
}

func (c *config) init(m map[string]any) {
	// set sane defaults
	if c.JobStatusCheckInterval == 0 {
		c.JobStatusCheckInterval = 2000
	}
	if c.JobTimeout == 0 {
		c.JobTimeout = 50000
	}
}

type config struct {
	Endpoint                  string                    `mapstructure:"endpoint"`
	AuthUser                  string                    `mapstructure:"auth_user"` // rclone basicauth user
	AuthPass                  string                    `mapstructure:"auth_pass"` // rclone basicauth pass
	AuthHeader                string                    `mapstructure:"auth_header"`
	JobStatusCheckInterval    int                       `mapstructure:"job_status_check_interval"`
	JobTimeout                int                       `mapstructure:"job_timeout"`
	Insecure                  bool                      `mapstructure:"insecure"`
	RemoveTransferJobOnCancel bool                      `mapstructure:"remove_transfer_job_on_cancel"`
	StorageDriver             string                    `mapstructure:"storagedriver"`
	StorageDrivers            map[string]map[string]any `mapstructure:"storagedrivers"`
}

type rclone struct {
	config   *config
	client   *httpclient.Client
	storage  repository.Repository
	monMu    sync.Mutex
	monitors map[string]context.CancelFunc
}

type rcloneHTTPErrorRes struct {
	Error  string         `json:"error"`
	Input  map[string]any `json:"input"`
	Path   string         `json:"path"`
	Status int            `json:"status"`
}

// rcloneRequestError is returned by callJSON when the rclone daemon answered
// with a non-200 response. It lets callers tell an explicit remote rejection
// apart from a response that never arrived (transport error).
type rcloneRequestError struct {
	Method string
	Status int
	Msg    string
}

func (e *rcloneRequestError) Error() string {
	return fmt.Sprintf("rclone driver: request %s responded with error, status: %v, error: %v", e.Method, e.Status, e.Msg)
}

const (
	rcloneSyncCopyMethod  = "/sync/copy"
	rcloneJobStatusMethod = "/job/status"
	rcloneJobStopMethod   = "/job/stop"
	rcloneJobListMethod   = "/job/list"
)

type endpoint struct {
	filePath       string
	endpoint       string
	endpointScheme string
	token          string
}

// New returns a new rclone driver.
func New(ctx context.Context, m map[string]any) (txdriver.Manager, error) {
	c, err := parseConfig(m)
	if err != nil {
		return nil, err
	}
	c.init(m)

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: c.Insecure}}
	client := httpclient.New(httpclient.RoundTripper(tr))

	storage, err := getStorageManager(ctx, c)
	if err != nil {
		return nil, err
	}

	d := &rclone{
		config:   c,
		client:   client,
		storage:  storage,
		monitors: map[string]context.CancelFunc{},
	}

	// Take over jobs from a previous run: every non-final job gets a monitor
	// again. Pending jobs whose remote start response was lost (jobid == -1)
	// go through identity-based recovery.
	jobs, err := storage.ListJobs()
	if err != nil {
		return nil, errors.Wrap(err, "rclone: error listing stored jobs on startup")
	}
	for _, job := range jobs {
		if repository.IsFinalStatus(job.TransferStatus) {
			continue
		}
		if job.JobID < 0 {
			pending := job
			go d.recoverPendingJob(context.Background(), pending)
		} else {
			d.ensureMonitor(job.TransferID, job.Generation, job.JobID)
		}
	}

	return d, nil
}

func parseConfig(m map[string]any) (*config, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		err = errors.Wrap(err, "error decoding conf")
		return nil, err
	}
	return c, nil
}

func getStorageManager(ctx context.Context, c *config) (repository.Repository, error) {
	if f, ok := repoRegistry.NewFuncs[c.StorageDriver]; ok {
		return f(ctx, c.StorageDrivers[c.StorageDriver])
	}
	return nil, errtypes.NotFound("rclone service: storage driver not found: " + c.StorageDriver)
}

// stableTransferID derives the stable transfer identity from the request,
// independently of credentials. Tokens may be renewed between retries, so
// they are not part of the identity; source/destination endpoints and paths
// (and the creator) are.
func stableTransferID(creator *userv1beta1.UserId, srcRemote, srcPath, dstHost, dstPath string) string {
	var b strings.Builder
	b.WriteString(creator.GetOpaqueId())
	b.WriteByte(0)
	b.WriteString(srcRemote)
	b.WriteByte(0)
	b.WriteString(srcPath)
	b.WriteByte(0)
	b.WriteString(dstHost)
	b.WriteByte(0)
	b.WriteString(dstPath)
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(b.String())).String()
}

// CreateTransfer creates a transfer job and returns a TxInfo object that includes a unique transfer id.
// Specified target URIs are of form scheme://userinfo@host:port?name={path}
func (driver *rclone) CreateTransfer(ctx context.Context, srcTargetURI string, dstTargetURI string) (*datatx.TxInfo, error) {
	log := appctx.GetLogger(ctx)
	srcEp, err := driver.extractEndpointInfo(ctx, srcTargetURI)
	if err != nil {
		return nil, err
	}
	srcRemote := fmt.Sprintf("%s://%s", srcEp.endpointScheme, srcEp.endpoint)
	srcPath := srcEp.filePath
	srcToken := srcEp.token

	destEp, err := driver.extractEndpointInfo(ctx, dstTargetURI)
	if err != nil {
		return nil, err
	}
	dstPath := destEp.filePath
	dstToken := destEp.token
	// destination host without embedded credentials
	dstHost := fmt.Sprintf("%s://%s", destEp.endpointScheme, destEp.endpoint)
	// we always set the userinfo part of the destination url for rclone tpc push support
	dstRemote := fmt.Sprintf("%s://%s@%s", destEp.endpointScheme, dstToken, destEp.endpoint)

	log.Debug().Str("srcRemote", srcRemote).Str("srcPath", srcPath).Str("srcToken", srcToken).Str("dstRemote", dstRemote).Str("dstPath", dstPath).Str("dstToken", dstToken).Msg("starting rclone job")

	creator, _ := appctx.ContextGetUser(ctx)
	txID := stableTransferID(creator.GetId(), srcRemote, srcPath, dstHost, dstPath)

	// same request retried: do not start another remote job, adopt the
	// existing one.
	if existing, err := driver.storage.GetJob(txID); err == nil {
		return driver.adoptTransfer(ctx, existing)
	}

	job := &repository.Job{
		TransferID:     txID,
		JobID:          -1,
		Generation:     1,
		TransferStatus: datatx.Status_STATUS_TRANSFER_NEW,
		SrcToken:       srcToken,
		SrcRemote:      srcRemote,
		SrcPath:        srcPath,
		DestToken:      dstToken,
		DestRemote:     dstRemote,
		DestPath:       dstPath,
		Ctime:          fmt.Sprint(time.Now().Unix()),
	}

	// publish the pending mapping, including both credentials and a queryable
	// status, before contacting rclone. Nothing exists remotely yet, so a
	// failure here is safe to retry.
	if err := driver.storage.StoreJob(job); err != nil {
		return driver.txInfo(job), errors.Wrap(err, "rclone: error publishing pending transfer mapping")
	}

	return driver.launch(ctx, job)
}

// adoptTransfer returns the state of an already stored transfer, taking it
// over (recovery + monitor) when needed, instead of starting a new job.
func (driver *rclone) adoptTransfer(ctx context.Context, job *repository.Job) (*datatx.TxInfo, error) {
	if repository.IsFinalStatus(job.TransferStatus) {
		return driver.finalTxInfo(job)
	}

	if job.JobID >= 0 {
		// make sure the transfer is being monitored (e.g. after a restart)
		driver.ensureMonitor(job.TransferID, job.Generation, job.JobID)
		return driver.txInfo(job), nil
	}

	// jobid unknown: the response of the remote start may have been lost
	recovered, err := driver.recoverPending(ctx, job)
	if err != nil {
		return driver.txInfo(recovered), err
	}
	if repository.IsFinalStatus(recovered.TransferStatus) {
		return driver.finalTxInfo(recovered)
	}
	return driver.txInfo(recovered), nil
}

// finalTxInfo returns the stored final state, without error when the transfer
// completed and with an error otherwise.
func (driver *rclone) finalTxInfo(job *repository.Job) (*datatx.TxInfo, error) {
	info := driver.txInfo(job)
	if job.TransferStatus == datatx.Status_STATUS_TRANSFER_COMPLETE {
		return info, nil
	}
	return info, errors.Errorf("rclone: transfer %s is already in final state %s", job.TransferID, job.TransferStatus)
}

// launch contacts the rclone daemon for a transfer whose pending mapping is
// already stored. The mapping with the actual remote job id is published
// before success is returned.
func (driver *rclone) launch(ctx context.Context, job *repository.Job) (*datatx.TxInfo, error) {
	log := appctx.GetLogger(ctx)

	// make sure the pending state is the published one
	job.JobID = -1
	job.TransferStatus = datatx.Status_STATUS_TRANSFER_NEW
	if err := driver.storage.StoreJob(job); err != nil {
		return driver.txInfo(job), errors.Wrap(err, "rclone: error publishing pending transfer mapping")
	}

	srcFs, dstFs := driver.buildFs(job)

	// stat source: keep directory / single-file distinction
	remotePathIsFolder, err := driver.remotePathIsFolder(job.SrcRemote, job.SrcPath, job.SrcToken)
	if err != nil {
		return driver.recordFailure(job, datatx.Status_STATUS_INVALID, errors.Wrap(err, "rclone: error stating src path"))
	}
	if !remotePathIsFolder {
		return driver.recordFailure(job, datatx.Status_STATUS_INVALID, errors.New("rclone: path is a file, only folder transfer is implemented"))
	}

	type rcloneAsyncReqJSON struct {
		SrcFs string `json:"srcFs"`
		DstFs string `json:"dstFs"`
		Async bool   `json:"_async"`
	}
	rcloneReq := &rcloneAsyncReqJSON{
		SrcFs: srcFs,
		DstFs: dstFs,
		Async: true,
	}

	type rcloneAsyncResJSON struct {
		JobID int64 `json:"jobid"`
	}
	var asyncRes rcloneAsyncResJSON
	if err := driver.callJSON(ctx, http.MethodPost, rcloneSyncCopyMethod, rcloneReq, &asyncRes); err != nil {
		var reqErr *rcloneRequestError
		if errors.As(err, &reqErr) {
			// explicit remote rejection: the response was received, persist
			// the failed state
			return driver.recordFailure(job, datatx.Status_STATUS_TRANSFER_FAILED,
				errors.Wrap(err, "rclone: remote rejected job start"))
		}
		// The daemon may have accepted the job even though the response could
		// not be received. Keep the pending record (jobid == -1): the next
		// request with the same identity queries the daemon instead of
		// starting another job.
		log.Warn().Err(err).Msg("rclone: error starting remote job, remote job state is unknown")
		return driver.txInfo(job), errors.Wrap(err, "rclone: error starting remote job, remote job state is unknown")
	}

	job.JobID = asyncRes.JobID
	if err := driver.storage.StoreJob(job); err != nil {
		// the local mapping could not be published: the user has not been
		// confirmed, cancel the remote job (best effort) and retain a failure
		// record
		log.Error().Err(err).Int64("jobid", job.JobID).Msg("rclone: error publishing remote job mapping, cancelling unconfirmed job")
		if stopErr := driver.stopRemote(context.Background(), job.JobID); stopErr != nil {
			log.Error().Err(stopErr).Msg("rclone: error cancelling unconfirmed remote job")
		}
		job.JobID = -1
		return driver.recordFailure(job, datatx.Status_STATUS_TRANSFER_FAILED,
			errors.Wrap(err, "rclone: error publishing remote job mapping, unconfirmed job has been cancelled"))
	}

	driver.ensureMonitor(job.TransferID, job.Generation, job.JobID)

	return driver.txInfo(job), nil
}

// recordFailure persists the given failure status for the job and returns the
// TxInfo together with the cause. The stored record is retained as failure
// cleanup.
func (driver *rclone) recordFailure(job *repository.Job, status datatx.Status, cause error) (*datatx.TxInfo, error) {
	job.TransferStatus = status
	if err := driver.storage.StoreJob(job); err != nil {
		cause = errors.Wrap(cause, err.Error())
	}
	return driver.txInfo(job), cause
}

// buildFs builds the rclone webdav fs specification strings, preserving the
// existing bearer / x-access-token authentication header handling.
func (driver *rclone) buildFs(job *repository.Job) (srcFs, dstFs string) {
	// bearer is the default authentication scheme for reva
	srcAuthHeader := fmt.Sprintf("bearer_token=\"%v\"", job.SrcToken)
	if driver.config.AuthHeader == "x-access-token" {
		srcAuthHeader = fmt.Sprintf("headers=\"x-access-token,%v\"", job.SrcToken)
	}
	srcFs = fmt.Sprintf(":webdav,%v,url=\"%v\":%v", srcAuthHeader, job.SrcRemote, job.SrcPath)

	destAuthHeader := fmt.Sprintf("bearer_token=\"%v\"", job.DestToken)
	if driver.config.AuthHeader == "x-access-token" {
		destAuthHeader = fmt.Sprintf("headers=\"x-access-token,%v\"", job.DestToken)
	}
	dstFs = fmt.Sprintf(":webdav,%v,url=\"%v\":%v", destAuthHeader, job.DestRemote, job.DestPath)
	return
}

// monitor periodically polls the status of one specific remote job generation
// until a final state is reached. Status writes are guarded by generation:
// a late status of an older remote job can never overwrite a newer generation
// and within a generation the first final state wins.
func (driver *rclone) monitor(ctx context.Context, transferID string, generation int64, remoteJobID int64) {
	log := appctx.GetLogger(ctx)
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		job, err := driver.storage.GetJob(transferID)
		if err != nil {
			// job was deleted
			return
		}
		if job.Generation != generation {
			// a newer transfer generation exists: this monitor is obsolete
			return
		}
		if repository.IsFinalStatus(job.TransferStatus) {
			log.Info().Msgf("rclone driver: transfer job endstatus reached: %v", job.TransferStatus)
			return
		}

		// check for timeout
		if time.Since(startTime).Milliseconds() > int64(driver.config.JobTimeout) {
			if _, err := driver.storage.TransitionStatus(job, datatx.Status_STATUS_TRANSFER_EXPIRED); err != nil {
				if isConflict(err) {
					return
				}
				// persistence failed: keep converging on the next iteration
				log.Error().Err(err).Msg("rclone driver: error saving expired status")
			} else {
				log.Info().Msg("rclone driver: transfer job timed out")
				return
			}
		}

		statusRes, err := driver.queryRemoteJobStatus(ctx, remoteJobID)
		if err != nil {
			// rclone-side problems remain a terminal invalid state as before
			log.Error().Err(err).Msg("rclone driver: error retrieving remote job status")
			if _, terr := driver.storage.TransitionStatus(job, datatx.Status_STATUS_INVALID); terr != nil {
				if isConflict(terr) {
					return
				}
				log.Error().Err(terr).Msg("rclone driver: error saving invalid status")
			} else {
				return
			}
			driver.wait(ctx)
			continue
		}

		var newStatus datatx.Status
		switch {
		case statusRes.Error != "":
			log.Error().Msgf("rclone driver: rclone responded with error: %v", statusRes.Error)
			newStatus = datatx.Status_STATUS_TRANSFER_FAILED
		case statusRes.Finished && statusRes.Success:
			log.Info().Msg("rclone driver: transfer job finished")
			newStatus = datatx.Status_STATUS_TRANSFER_COMPLETE
		case statusRes.Finished && !statusRes.Success:
			log.Info().Msg("rclone driver: transfer job failed")
			newStatus = datatx.Status_STATUS_TRANSFER_FAILED
		default:
			log.Info().Msg("rclone driver: transfer job in progress")
			newStatus = datatx.Status_STATUS_TRANSFER_IN_PROGRESS
		}

		if _, err := driver.storage.TransitionStatus(job, newStatus); err != nil {
			if isConflict(err) {
				// newer generation or a final state was already recorded:
				// that state is the unique final state
				return
			}
			// could not persist the observed result (e.g. disk error): do not
			// fabricate nor discard it, keep converging the existing result
			log.Error().Err(err).Msg("rclone driver: error saving transfer status, will retry convergence")
		} else if repository.IsFinalStatus(newStatus) {
			return
		}

		driver.wait(ctx)
	}
}

// isConflict reports whether the transition error denotes a state conflict
// (generation mismatch or final state already recorded).
func isConflict(err error) bool {
	return errors.Is(err, repository.ErrGenerationConflict) || errors.Is(err, repository.ErrAlreadyFinal)
}

func (driver *rclone) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond * time.Duration(driver.config.JobStatusCheckInterval)):
	}
}

type rcloneStatusResJSON struct {
	Finished  bool    `json:"finished"`
	Success   bool    `json:"success"`
	ID        int64   `json:"id"`
	Error     string  `json:"error"`
	Group     string  `json:"group"`
	StartTime string  `json:"startTime"`
	EndTime   string  `json:"endTime"`
	Duration  float64 `json:"duration"`
}

func (driver *rclone) queryRemoteJobStatus(ctx context.Context, remoteJobID int64) (*rcloneStatusResJSON, error) {
	type rcloneStatusReqJSON struct {
		JobID int64 `json:"jobid"`
	}
	var resData rcloneStatusResJSON
	if err := driver.callJSON(ctx, http.MethodPost, rcloneJobStatusMethod, &rcloneStatusReqJSON{JobID: remoteJobID}, &resData); err != nil {
		return nil, err
	}
	return &resData, nil
}

// recoverPending resolves a stored job whose remote start response was lost
// (jobid == -1). It queries the rclone daemon by identity: if a matching
// remote job exists it is claimed, otherwise the remote job is started now.
func (driver *rclone) recoverPending(ctx context.Context, job *repository.Job) (*repository.Job, error) {
	return driver.recoverPendingJobResult(ctx, job)
}

// recoverPendingJob is the background variant used on startup: errors are
// only logged.
func (driver *rclone) recoverPendingJob(ctx context.Context, job *repository.Job) {
	log := appctx.GetLogger(ctx)
	if _, err := driver.recoverPendingJobResult(ctx, job); err != nil {
		log.Error().Err(err).Str("txID", job.TransferID).Msg("rclone: error recovering pending job")
	}
}

func (driver *rclone) recoverPendingJobResult(ctx context.Context, job *repository.Job) (*repository.Job, error) {
	log := appctx.GetLogger(ctx)

	ids, err := driver.listRemoteJobs(ctx)
	if err != nil {
		return job, errors.Wrap(err, "rclone: could not query remote jobs while recovering pending transfer")
	}

	ctime, _ := strconv.ParseInt(job.Ctime, 10, 64)

	// look for the job started right after the pending mapping was published.
	// rclone job ids are monotonic; with a dedicated rclone daemon the latest
	// matching id is ours.
	var candidate *rcloneStatusResJSON
	for _, id := range ids {
		st, err := driver.queryRemoteJobStatus(ctx, id)
		if err != nil {
			continue
		}
		start, err := time.Parse(time.RFC3339, st.StartTime)
		if err != nil {
			continue
		}
		// small clock tolerance
		if start.Unix() < ctime-2 {
			continue
		}
		if candidate == nil || st.ID > candidate.ID {
			candidate = st
		}
	}

	if candidate == nil {
		// no remote job found: either the job was never created or the rclone
		// daemon was restarted (job state is in-memory, so the old job is
		// gone). It is safe to start it now under the same generation.
		log.Info().Str("txID", job.TransferID).Msg("rclone: no remote job found for pending transfer, starting now")
		info, launchErr := driver.launch(ctx, job)
		if launchErr != nil {
			return job, launchErr
		}
		updated, getErr := driver.storage.GetJob(info.GetId().OpaqueId)
		if getErr != nil {
			return job, getErr
		}
		return updated, nil
	}

	// claim the discovered job
	job.JobID = candidate.ID
	switch {
	case candidate.Error != "":
		job.TransferStatus = datatx.Status_STATUS_TRANSFER_FAILED
	case candidate.Finished && candidate.Success:
		job.TransferStatus = datatx.Status_STATUS_TRANSFER_COMPLETE
	case candidate.Finished && !candidate.Success:
		job.TransferStatus = datatx.Status_STATUS_TRANSFER_FAILED
	default:
		job.TransferStatus = datatx.Status_STATUS_TRANSFER_IN_PROGRESS
	}
	if err := driver.storage.StoreJob(job); err != nil {
		return job, errors.Wrap(err, "rclone: error publishing recovered job mapping")
	}
	if !repository.IsFinalStatus(job.TransferStatus) {
		driver.ensureMonitor(job.TransferID, job.Generation, job.JobID)
	}
	return job, nil
}

func (driver *rclone) listRemoteJobs(ctx context.Context) ([]int64, error) {
	var resData struct {
		JobIDs []int64 `json:"jobids"`
	}
	if err := driver.callJSON(ctx, http.MethodPost, rcloneJobListMethod, struct{}{}, &resData); err != nil {
		return nil, err
	}
	return resData.JobIDs, nil
}

func (driver *rclone) stopRemote(ctx context.Context, remoteJobID int64) error {
	_, err := driver.stopRemoteResult(ctx, remoteJobID)
	return err
}

func (driver *rclone) stopRemoteResult(ctx context.Context, remoteJobID int64) (*rcloneStatusResJSON, error) {
	type rcloneStopRequest struct {
		JobID int64 `json:"jobid"`
	}
	var resData rcloneStatusResJSON
	if err := driver.callJSON(ctx, http.MethodPost, rcloneJobStopMethod, &rcloneStopRequest{JobID: remoteJobID}, &resData); err != nil {
		return nil, err
	}
	return &resData, nil
}

// callJSON performs an authenticated json request against the rclone daemon
// and decodes the successful response into resBody (which may be nil).
func (driver *rclone) callJSON(ctx context.Context, method, apiMethod string, reqBody any, resBody any) error {
	data, err := json.Marshal(reqBody)
	if err != nil {
		return errors.Wrap(err, "rclone driver: error marshalling request data")
	}

	u, err := url.Parse(driver.config.Endpoint)
	if err != nil {
		return errors.Wrap(err, "rclone driver: error parsing driver endpoint")
	}
	u.Path = path.Join(u.Path, apiMethod)
	requestURL := u.String()

	req, err := http.NewRequestWithContext(ctx, method, requestURL, bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "rclone driver: error framing request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(driver.config.AuthUser, driver.config.AuthPass)

	res, err := driver.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "rclone driver: error sending request")
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var errorResData rcloneHTTPErrorRes
		if decErr := json.NewDecoder(res.Body).Decode(&errorResData); decErr != nil {
			return &rcloneRequestError{Method: apiMethod, Status: res.StatusCode, Msg: "unknown error"}
		}
		return &rcloneRequestError{Method: apiMethod, Status: errorResData.Status, Msg: errorResData.Error}
	}

	if resBody != nil {
		if err := json.NewDecoder(res.Body).Decode(resBody); err != nil {
			return errors.Wrap(err, "rclone driver: error decoding response data")
		}
	}
	return nil
}

func (driver *rclone) txInfo(job *repository.Job) *datatx.TxInfo {
	seconds, _ := strconv.ParseInt(job.Ctime, 10, 64)
	return &datatx.TxInfo{
		Id:     &datatx.TxId{OpaqueId: job.TransferID},
		Status: job.TransferStatus,
		Ctime:  &typespb.Timestamp{Seconds: uint64(seconds)},
	}
}

// ensureMonitor starts the status polling goroutine for the given transfer
// generation, unless one is already running.
func (driver *rclone) ensureMonitor(transferID string, generation, remoteJobID int64) {
	key := monitorKey(transferID, generation)

	driver.monMu.Lock()
	defer driver.monMu.Unlock()
	if _, ok := driver.monitors[key]; ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	driver.monitors[key] = cancel

	go func() {
		defer func() {
			driver.monMu.Lock()
			if cur, ok := driver.monitors[key]; ok && cur != nil {
				delete(driver.monitors, key)
			}
			driver.monMu.Unlock()
			cancel()
		}()
		driver.monitor(ctx, transferID, generation, remoteJobID)
	}()
}

func monitorKey(transferID string, generation int64) string {
	return transferID + "#" + strconv.FormatInt(generation, 10)
}

// GetTransferStatus returns the status of the transfer with the specified job id.
func (driver *rclone) GetTransferStatus(ctx context.Context, transferID string) (*datatx.TxInfo, error) {
	job, err := driver.storage.GetJob(transferID)
	if err != nil {
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_INVALID,
			Ctime:  nil,
		}, err
	}
	return driver.txInfo(job), nil
}

// CancelTransfer cancels the transfer with the specified transfer id.
func (driver *rclone) CancelTransfer(ctx context.Context, transferID string) (*datatx.TxInfo, error) {
	job, err := driver.storage.GetJob(transferID)
	if err != nil {
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_INVALID,
			Ctime:  nil,
		}, err
	}

	cTime, _ := strconv.ParseInt(job.Ctime, 10, 64)
	// rclone cancel may fail so remove job from model first to be sure
	transferRemovedMessage := ""
	if driver.config.RemoveTransferJobOnCancel {
		if err := driver.storage.DeleteJob(job); err != nil {
			return &datatx.TxInfo{
				Id:     &datatx.TxId{OpaqueId: transferID},
				Status: datatx.Status_STATUS_INVALID,
				Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
			}, err
		}
		transferRemovedMessage = "(transfer job successfully removed)"
	}

	if repository.IsFinalStatus(job.TransferStatus) {
		err := errors.Wrap(errors.New("rclone driver: job already in end state"), transferRemovedMessage)
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_INVALID,
			Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
		}, err
	}

	stopRes, err := driver.stopRemoteResult(ctx, job.JobID)
	if err != nil {
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_INVALID,
			Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
		}, errors.Wrap(err, transferRemovedMessage)
	}

	if stopRes.Error != "" {
		// same behavior as before: report cancel failure without persisting
		// (the rclone job ended with an error)
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_TRANSFER_CANCEL_FAILED,
			Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
		}, errors.Wrap(errors.New(stopRes.Error), transferRemovedMessage)
	}

	// only update when job's not removed
	if !driver.config.RemoveTransferJobOnCancel {
		current, terr := driver.storage.TransitionStatus(job, datatx.Status_STATUS_TRANSFER_CANCELLED)
		if terr != nil {
			// cancel and completion interleaved: return the unique state that
			// was recorded first instead of overwriting it
			if current != nil {
				return driver.txInfo(current), errors.Wrap(errors.Errorf("rclone driver: job already in end state %v", current.TransferStatus), transferRemovedMessage)
			}
			return &datatx.TxInfo{
				Id:     &datatx.TxId{OpaqueId: transferID},
				Status: datatx.Status_STATUS_INVALID,
				Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
			}, errors.Wrap(terr, transferRemovedMessage)
		}
	}

	return &datatx.TxInfo{
		Id:     &datatx.TxId{OpaqueId: transferID},
		Status: datatx.Status_STATUS_TRANSFER_CANCELLED,
		Ctime:  &typespb.Timestamp{Seconds: uint64(cTime)},
	}, nil
}

// RetryTransfer retries the transfer with the specified transfer ID.
// Note that tokens must still be valid.
func (driver *rclone) RetryTransfer(ctx context.Context, transferID string) (*datatx.TxInfo, error) {
	job, err := driver.storage.GetJob(transferID)
	if err != nil {
		return &datatx.TxInfo{
			Id:     &datatx.TxId{OpaqueId: transferID},
			Status: datatx.Status_STATUS_INVALID,
			Ctime:  nil,
		}, errors.Wrapf(err, "rclone: error retrying transfer (transferID: %s)", transferID)
	}

	// the previous generation must have reached a final state
	if !repository.IsFinalStatus(job.TransferStatus) {
		return driver.txInfo(job), errors.New("rclone: job still running, unable to restart")
	}

	// publish the new generation before contacting the daemon. Late status
	// updates of the old remote job are now rejected by the generation guard.
	job.Generation++
	job.JobID = -1
	job.TransferStatus = datatx.Status_STATUS_TRANSFER_NEW
	if err := driver.storage.StoreJob(job); err != nil {
		return driver.txInfo(job), errors.Wrap(err, "rclone: error publishing new transfer generation")
	}

	return driver.launch(ctx, job)
}

func (driver *rclone) remotePathIsFolder(remote string, remotePath string, remoteToken string) (bool, error) {
	type rcloneListReqJSON struct {
		Fs     string `json:"fs"`
		Remote string `json:"remote"`
	}
	fs := fmt.Sprintf(":webdav,headers=\"x-access-token,%v\",url=\"%v\":", remoteToken, remote)
	rcloneReq := &rcloneListReqJSON{
		Fs:     fs,
		Remote: remotePath,
	}

	type item struct {
		Path     string `json:"Path"`
		Name     string `json:"Name"`
		Size     int64  `json:"Size"`
		MimeType string `json:"MimeType"`
		ModTime  string `json:"ModTime"`
		IsDir    bool   `json:"IsDir"`
	}
	var resData struct {
		List []*item `json:"list"`
	}
	if err := driver.callJSON(context.Background(), http.MethodPost, "/operations/list", rcloneReq, &resData); err != nil {
		return false, err
	}

	// a file will return one single item, the file, with path being the remote path and IsDir will be false
	if len(resData.List) == 1 && resData.List[0].Path == remotePath && !resData.List[0].IsDir {
		return false, nil
	}

	// in all other cases the remote path is a directory
	return true, nil
}

func (driver *rclone) extractEndpointInfo(ctx context.Context, targetURL string) (*endpoint, error) {
	if targetURL == "" {
		return nil, errtypes.BadRequest("datatx service: ref target is an empty uri")
	}

	uri, err := url.Parse(targetURL)
	if err != nil {
		return nil, errors.Wrap(err, "datatx service: error parsing target uri: "+targetURL)
	}

	m, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, errors.Wrap(err, "datatx service: error parsing target resource name")
	}

	var filePath string
	if m["name"] != nil {
		filePath = m["name"][0]
	}

	return &endpoint{
		filePath:       filePath,
		endpoint:       uri.Host + uri.Path,
		endpointScheme: uri.Scheme,
		token:          uri.User.String(),
	}, nil
}
