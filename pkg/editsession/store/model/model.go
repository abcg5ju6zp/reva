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

// Package model holds the GORM models of the editsession SQL store.
package model

import "time"

// GenerationModel is the persisted editing generation.
type GenerationModel struct {
	ResourceKey string `gorm:"column:resource_key;uniqueIndex:idx_generation_resource_seq,priority:1"`
	Sequence    int64  `gorm:"column:sequence;uniqueIndex:idx_generation_resource_seq,priority:2"`
	OldPath     string `gorm:"column:old_path"`
	NewPath     string `gorm:"column:new_path"`
	State       string `gorm:"column:state;index"`
	// Confirmations is the JSON-encoded map[party]bool.
	Confirmations string     `gorm:"column:confirmations"`
	SupersededBy  int64      `gorm:"column:superseded_by"`
	CommittedAt   *time.Time `gorm:"column:committed_at"`
	CreatedAt     time.Time  `gorm:"column:created_at"`
	UpdatedAt     time.Time  `gorm:"column:updated_at"`
}

// TableName gives the table name.
func (GenerationModel) TableName() string { return "editing_generations" }

// SessionModel is the persisted editing session.
type SessionModel struct {
	ID          string `gorm:"column:id;primaryKey"`
	ResourceKey string `gorm:"column:resource_key;index:idx_session_resource"`
	Generation  int64  `gorm:"column:generation"`
	// UserJSON is the JSON-encoded user id.
	UserJSON    string     `gorm:"column:user"`
	DisplayName string     `gorm:"column:display_name"`
	AppProvider string     `gorm:"column:app_provider"`
	LockID      string     `gorm:"column:lock_id"`
	DownloadID  string     `gorm:"column:download_id"`
	UploadID    string     `gorm:"column:upload_id"`
	State       string     `gorm:"column:state"`
	OpenedAt    time.Time  `gorm:"column:opened_at"`
	ClosedAt    *time.Time `gorm:"column:closed_at"`
}

// TableName gives the table name.
func (SessionModel) TableName() string { return "editing_sessions" }

// UploadModel is the persisted in-flight upload (save) registration.
type UploadModel struct {
	ID          string     `gorm:"column:id;primaryKey"`
	ResourceKey string     `gorm:"column:resource_key;index:idx_upload_resource"`
	Generation  int64      `gorm:"column:generation"`
	SessionID   string     `gorm:"column:session_id"`
	State       string     `gorm:"column:state"`
	StartedAt   time.Time  `gorm:"column:started_at"`
	EndedAt     *time.Time `gorm:"column:ended_at"`
}

// TableName gives the table name.
func (UploadModel) TableName() string { return "editing_uploads" }

// CallbackModel records an applied app callback for idempotency. The
// dedup identity includes the kind, so a saved and a closed event with the
// same sequence are distinct callbacks.
type CallbackModel struct {
	ID          int64  `gorm:"column:id;primaryKey;autoIncrement"`
	ResourceKey string `gorm:"column:resource_key;uniqueIndex:idx_callback_dedup,priority:1"`
	SessionID   string `gorm:"column:session_id;uniqueIndex:idx_callback_dedup,priority:2"`
	Seq         int64  `gorm:"column:seq;uniqueIndex:idx_callback_dedup,priority:3"`
	Kind        string `gorm:"column:kind;uniqueIndex:idx_callback_dedup,priority:4"`
}

// TableName gives the table name.
func (CallbackModel) TableName() string { return "editing_callbacks" }
