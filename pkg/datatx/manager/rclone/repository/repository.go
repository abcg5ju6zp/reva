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

package repository

import (
	"errors"

	datatx "github.com/cs3org/go-cs3apis/cs3/tx/v1beta1"
)

// Job represents transfer job.
type Job struct {
	TransferID     string
	JobID          int64
	// Generation is incremented every time a new remote rclone job is started
	// for the same TransferID (initial creation and every retry). It is used
	// to make sure late status updates of an older remote job can never
	// overwrite the state of a newer generation.
	Generation     int64
	TransferStatus datatx.Status
	SrcToken       string
	SrcRemote      string
	SrcPath        string
	DestToken      string
	DestRemote     string
	DestPath       string
	Ctime          string
}

// Sentinel errors returned by Repository implementations. Callers are expected
// to check with errors.Is.
var (
	// ErrJobNotFound is returned when no job is stored under the given transfer id.
	ErrJobNotFound = errors.New("rclone repository: job not found")
	// ErrGenerationConflict is returned when the stored job belongs to a newer
	// generation than the one referenced by the caller.
	ErrGenerationConflict = errors.New("rclone repository: transfer generation conflict")
	// ErrAlreadyFinal is returned when trying to move a job that is already
	// in a final state to another (final) state. Within one generation the
	// first observed final state wins.
	ErrAlreadyFinal = errors.New("rclone repository: job is already in a final state")
)

// FinalStatuses contains the statuses that cannot be changed anymore once reached.
var FinalStatuses = map[datatx.Status]struct{}{
	datatx.Status_STATUS_INVALID:                {},
	datatx.Status_STATUS_DESTINATION_NOT_FOUND:  {},
	datatx.Status_STATUS_TRANSFER_COMPLETE:      {},
	datatx.Status_STATUS_TRANSFER_FAILED:        {},
	datatx.Status_STATUS_TRANSFER_CANCELLED:     {},
	datatx.Status_STATUS_TRANSFER_CANCEL_FAILED: {},
	datatx.Status_STATUS_TRANSFER_EXPIRED:       {},
}

// IsFinalStatus returns true if the given status cannot be changed anymore.
func IsFinalStatus(s datatx.Status) bool {
	_, ok := FinalStatuses[s]
	return ok
}

// Clone returns a deep copy of the job.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	clone := *j
	return &clone
}

// Repository the interface that any storage driver should implement.
type Repository interface {
	StoreJob(job *Job) error
	GetJob(transferID string) (*Job, error)
	DeleteJob(job *Job) error
	// ListJobs returns all stored jobs.
	ListJobs() ([]*Job, error)
	// TransitionStatus conditionally updates the status of a job.
	//
	// The transition succeeds only when all of the following hold:
	//   - a job with job.TransferID is stored
	//   - the stored generation equals job.Generation
	//   - when newStatus is final, the stored status is not already final
	//   - a final status is never moved back to a non-final status
	//
	// It returns the state of the stored job and one of the sentinel errors
	// defined in this package on conflict.
	TransitionStatus(job *Job, newStatus datatx.Status) (*Job, error)
}
