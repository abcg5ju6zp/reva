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

// Package sql is the GORM-backed implementation of the editsession store.
package sql

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/cmd/revad/pkg/config"
	"github.com/cs3org/reva/v3/pkg/editsession"
	"github.com/cs3org/reva/v3/pkg/editsession/store/model"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/sharedconf"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// Config holds the store configuration.
type Config struct {
	config.Database `mapstructure:",squash"`
}

// ApplyDefaults fills defaults.
func (c *Config) ApplyDefaults() {
	c.Database = sharedconf.GetDBInfo(c.Database)
}

type store struct {
	db *gorm.DB
}

// New returns a GORM-backed editsession store.
func New(ctx context.Context, m map[string]any) (editsession.Store, error) {
	var c Config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}
	return NewWithConfig(ctx, &c)
}

// NewWithConfig returns a store built from the given config.
func NewWithConfig(ctx context.Context, c *Config) (editsession.Store, error) {
	db, err := getDB(c)
	if err != nil {
		return nil, fmt.Errorf("editsession sql: opening database: %w", err)
	}
	if err := db.AutoMigrate(&model.GenerationModel{}, &model.SessionModel{}, &model.UploadModel{}, &model.CallbackModel{}); err != nil {
		return nil, fmt.Errorf("editsession sql: migrating schema: %w", err)
	}
	return &store{db: db}, nil
}

func getDB(c *Config) (*gorm.DB, error) {
	// translate driver constraint errors into gorm.ErrDuplicatedKey so the
	// callers can classify duplicates without driver-specific parsing.
	gormCfg := &gorm.Config{TranslateError: true}
	switch c.Engine {
	case "sqlite":
		// busy_timeout lets concurrent writers wait instead of failing with
		// "database is locked"; a single open connection serializes writes.
		dsn := c.DBName + "?_busy_timeout=5000"
		db, err := gorm.Open(sqlite.Open(dsn), gormCfg)
		if err == nil {
			sqlDB, dErr := db.DB()
			if dErr == nil {
				sqlDB.SetMaxOpenConns(1)
			}
		}
		return db, err
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		return gorm.Open(mysql.Open(dsn), gormCfg)
	default:
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		return gorm.Open(mysql.Open(dsn), gormCfg)
	}
}

// --- conversions ---

func toGenerationModel(g *editsession.Generation) (*model.GenerationModel, error) {
	conf := map[string]bool{}
	for k, v := range g.Confirmations {
		conf[string(k)] = v
	}
	confJSON, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}
	return &model.GenerationModel{
		ResourceKey:   g.Resource.Key(),
		Sequence:      g.Sequence,
		OldPath:       g.OldPath,
		NewPath:       g.NewPath,
		State:         string(g.State),
		Confirmations: string(confJSON),
		SupersededBy:  g.SupersededBy,
		CommittedAt:   g.CommittedAt,
		CreatedAt:     g.CreatedAt,
		UpdatedAt:     g.UpdatedAt,
	}, nil
}

func fromGenerationModel(m *model.GenerationModel) (*editsession.Generation, error) {
	parts := splitResourceKey(m.ResourceKey)
	conf := map[string]bool{}
	if m.Confirmations != "" {
		if err := json.Unmarshal([]byte(m.Confirmations), &conf); err != nil {
			return nil, err
		}
	}
	confirmations := map[editsession.Party]bool{}
	for k, v := range conf {
		confirmations[editsession.Party(k)] = v
	}
	return &editsession.Generation{
		Sequence:      m.Sequence,
		Resource:      parts,
		OldPath:       m.OldPath,
		NewPath:       m.NewPath,
		State:         editsession.GenState(m.State),
		Confirmations: confirmations,
		SupersededBy:  m.SupersededBy,
		CommittedAt:   m.CommittedAt,
		CreatedAt:     m.CreatedAt,
		UpdatedAt:     m.UpdatedAt,
	}, nil
}

// splitResourceKey decodes a key produced by ResourceID.Key (base64-URL of
// the NUL-joined triple).
func splitResourceKey(key string) editsession.ResourceID {
	var r editsession.ResourceID
	decoded, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return r
	}
	for i, part := range strings.Split(string(decoded), "\x00") {
		switch i {
		case 0:
			r.StorageID = part
		case 1:
			r.SpaceID = part
		case 2:
			r.OpaqueID = part
		}
	}
	return r
}

func toSessionModel(s *editsession.Session) (*model.SessionModel, error) {
	var userJSON string
	if s.User != nil {
		b, err := json.Marshal(s.User)
		if err != nil {
			return nil, err
		}
		userJSON = string(b)
	}
	return &model.SessionModel{
		ID:          s.ID,
		ResourceKey: s.Resource.Key(),
		Generation:  s.Generation,
		UserJSON:    userJSON,
		DisplayName: s.DisplayName,
		AppProvider: s.AppProvider,
		LockID:      s.LockID,
		DownloadID:  s.DownloadID,
		UploadID:    s.UploadID,
		State:       string(s.State),
		OpenedAt:    s.OpenedAt,
		ClosedAt:    s.ClosedAt,
	}, nil
}

func fromSessionModel(m *model.SessionModel) (*editsession.Session, error) {
	parts := splitResourceKey(m.ResourceKey)
	var user *userpb.UserId
	if m.UserJSON != "" {
		user = &userpb.UserId{}
		if err := json.Unmarshal([]byte(m.UserJSON), user); err != nil {
			return nil, err
		}
	}
	return &editsession.Session{
		ID:          m.ID,
		Resource:    parts,
		Generation:  m.Generation,
		User:        user,
		DisplayName: m.DisplayName,
		AppProvider: m.AppProvider,
		LockID:      m.LockID,
		DownloadID:  m.DownloadID,
		UploadID:    m.UploadID,
		State:       editsession.SessionState(m.State),
		OpenedAt:    m.OpenedAt,
		ClosedAt:    m.ClosedAt,
	}, nil
}

func toUploadModel(u *editsession.InFlightUpload) *model.UploadModel {
	return &model.UploadModel{
		ID:          u.ID,
		ResourceKey: u.Resource.Key(),
		Generation:  u.Generation,
		SessionID:   u.SessionID,
		State:       string(u.State),
		StartedAt:   u.StartedAt,
		EndedAt:     u.EndedAt,
	}
}

func fromUploadModel(m *model.UploadModel) *editsession.InFlightUpload {
	return &editsession.InFlightUpload{
		ID:         m.ID,
		Resource:   splitResourceKey(m.ResourceKey),
		Generation: m.Generation,
		SessionID:  m.SessionID,
		State:      editsession.UploadState(m.State),
		StartedAt:  m.StartedAt,
		EndedAt:    m.EndedAt,
	}
}

// --- generations ---

func (s *store) CreateGeneration(ctx context.Context, g *editsession.Generation) error {
	row, err := toGenerationModel(g)
	if err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Create(row).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return errtypes.AlreadyExists("editsession: generation already exists: " + g.Resource.Key())
		}
		return fmt.Errorf("editsession: creating generation: %w", err)
	}
	return nil
}

func (s *store) GetGeneration(ctx context.Context, r editsession.ResourceID, seq int64) (*editsession.Generation, error) {
	row := &model.GenerationModel{}
	err := s.db.WithContext(ctx).Where("resource_key = ? AND sequence = ?", r.Key(), seq).First(row).Error
	if err != nil {
		return nil, mapQueryError(err, "generation")
	}
	return fromGenerationModel(row)
}

func (s *store) GetInProgressGeneration(ctx context.Context, r editsession.ResourceID) (*editsession.Generation, error) {
	rows := []*model.GenerationModel{}
	err := s.db.WithContext(ctx).
		Where("resource_key = ? AND state IN ?", r.Key(), []string{string(editsession.GenFreezing), string(editsession.GenMoving)}).
		Order("sequence DESC").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("editsession: looking up in-progress generation: %w", err)
	}
	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		return fromGenerationModel(rows[0])
	default:
		return nil, errtypes.InternalError("editsession: more than one in-progress generation for " + r.Key())
	}
}

func (s *store) ListGenerations(ctx context.Context, r editsession.ResourceID) ([]*editsession.Generation, error) {
	rows := []*model.GenerationModel{}
	err := s.db.WithContext(ctx).Where("resource_key = ?", r.Key()).Order("sequence ASC").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("editsession: listing generations: %w", err)
	}
	out := make([]*editsession.Generation, 0, len(rows))
	for _, row := range rows {
		g, err := fromGenerationModel(row)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *store) TransitionGeneration(ctx context.Context, r editsession.ResourceID, seq int64, from []editsession.GenState, to editsession.GenState) (*editsession.Generation, error) {
	// the target state must be a legal successor of at least one of the
	// expected current states.
	legalTarget := false
	for _, st := range from {
		if st.CanTransitionTo(to) {
			legalTarget = true
			break
		}
	}
	if !legalTarget {
		return nil, errtypes.BadRequest(fmt.Sprintf("editsession: illegal transition to %s from states %v", to, from))
	}

	fromStates := make([]string, len(from))
	for i, st := range from {
		fromStates[i] = string(st)
	}

	now := time.Now()
	updates := map[string]any{
		"state":      string(to),
		"updated_at": now,
	}
	if to == editsession.GenConfirmed {
		updates["committed_at"] = now
	}

	res := s.db.WithContext(ctx).Model(&model.GenerationModel{}).
		Where("resource_key = ? AND sequence = ? AND state IN ?", r.Key(), seq, fromStates).
		Updates(updates)
	if res.Error != nil {
		return nil, fmt.Errorf("editsession: transitioning generation: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return nil, errtypes.Conflict(fmt.Sprintf("editsession: generation %d of %s not in states %v", seq, r.Key(), from))
	}
	return s.GetGeneration(ctx, r, seq)
}

func (s *store) MarkSuperseded(ctx context.Context, r editsession.ResourceID, seq, by int64) error {
	res := s.db.WithContext(ctx).Model(&model.GenerationModel{}).
		Where("resource_key = ? AND sequence = ?", r.Key(), seq).
		Update("superseded_by", by)
	if res.Error != nil {
		return fmt.Errorf("editsession: marking generation superseded: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return errtypes.NotFound(fmt.Sprintf("editsession: generation %d of %s", seq, r.Key()))
	}
	return nil
}

func (s *store) ClearSuperseded(ctx context.Context, r editsession.ResourceID, seq int64) error {
	return s.db.WithContext(ctx).Model(&model.GenerationModel{}).
		Where("resource_key = ? AND sequence = ?", r.Key(), seq).
		Update("superseded_by", 0).Error
}

// ClearSupersededIf clears the marker only while it points at
// expectedSuperseder; no error (and no write) when it does not match.
func (s *store) ClearSupersededIf(ctx context.Context, r editsession.ResourceID, seq, expectedSuperseder int64) error {
	return s.db.WithContext(ctx).Model(&model.GenerationModel{}).
		Where("resource_key = ? AND sequence = ? AND superseded_by = ?",
			r.Key(), seq, expectedSuperseder).
		Update("superseded_by", 0).Error
}

func (s *store) RecordConfirmation(ctx context.Context, r editsession.ResourceID, seq int64, party editsession.Party) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row := &model.GenerationModel{}
		if err := tx.Where("resource_key = ? AND sequence = ?", r.Key(), seq).First(row).Error; err != nil {
			return mapQueryError(err, "generation")
		}
		if row.State != string(editsession.GenMoving) {
			return errtypes.BadRequest(fmt.Sprintf("editsession: generation %d of %s not in moving state", seq, r.Key()))
		}
		confirmations := map[string]bool{}
		if row.Confirmations != "" {
			if err := json.Unmarshal([]byte(row.Confirmations), &confirmations); err != nil {
				return err
			}
		}
		confirmations[string(party)] = true
		encoded, err := json.Marshal(confirmations)
		if err != nil {
			return err
		}
		return tx.Model(&model.GenerationModel{}).
			Where("resource_key = ? AND sequence = ?", r.Key(), seq).
			Updates(map[string]any{
				"confirmations": string(encoded),
				"updated_at":    time.Now(),
			}).Error
	})
}

func (s *store) ListRecoverableGenerations(ctx context.Context) ([]*editsession.Generation, error) {
	rows := []*model.GenerationModel{}
	err := s.db.WithContext(ctx).
		Where("state IN ?", []string{string(editsession.GenFreezing), string(editsession.GenMoving)}).
		Order("resource_key, sequence").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("editsession: listing recoverable generations: %w", err)
	}
	out := make([]*editsession.Generation, 0, len(rows))
	for _, row := range rows {
		g, err := fromGenerationModel(row)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

// ListAbortedGenerations implements editsession.Store.
func (s *store) ListAbortedGenerations(ctx context.Context) ([]*editsession.Generation, error) {
	rows := []*model.GenerationModel{}
	err := s.db.WithContext(ctx).
		Where("state = ?", string(editsession.GenAborted)).
		Order("resource_key, sequence").Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("editsession: listing aborted generations: %w", err)
	}
	out := make([]*editsession.Generation, 0, len(rows))
	for _, row := range rows {
		g, err := fromGenerationModel(row)
		if err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, nil
}

func (s *store) DeleteGeneration(ctx context.Context, r editsession.ResourceID, seq int64) error {
	res := s.db.WithContext(ctx).Where("resource_key = ? AND sequence = ?", r.Key(), seq).Delete(&model.GenerationModel{})
	if res.Error != nil {
		return fmt.Errorf("editsession: deleting generation: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return errtypes.NotFound(fmt.Sprintf("editsession: generation %d of %s", seq, r.Key()))
	}
	return nil
}

// --- sessions ---

func (s *store) CreateSession(ctx context.Context, sess *editsession.Session) error {
	row, err := toSessionModel(sess)
	if err != nil {
		return err
	}
	if err := s.db.WithContext(ctx).Create(row).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return errtypes.AlreadyExists("editsession: session already exists: " + sess.ID)
		}
		return fmt.Errorf("editsession: creating session: %w", err)
	}
	return nil
}

func (s *store) GetSession(ctx context.Context, id string) (*editsession.Session, error) {
	row := &model.SessionModel{}
	err := s.db.WithContext(ctx).Where("id = ?", id).First(row).Error
	if err != nil {
		return nil, mapQueryError(err, "session")
	}
	return fromSessionModel(row)
}

func (s *store) ListSessions(ctx context.Context, r editsession.ResourceID, genSeq int64, openOnly bool) ([]*editsession.Session, error) {
	q := s.db.WithContext(ctx).Model(&model.SessionModel{}).Where("resource_key = ?", r.Key())
	if genSeq > 0 {
		q = q.Where("generation = ?", genSeq)
	}
	if openOnly {
		q = q.Where("state = ?", string(editsession.SessionActive))
	}
	rows := []*model.SessionModel{}
	if err := q.Order("opened_at").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("editsession: listing sessions: %w", err)
	}
	out := make([]*editsession.Session, 0, len(rows))
	for _, row := range rows {
		sess, err := fromSessionModel(row)
		if err != nil {
			return nil, err
		}
		out = append(out, sess)
	}
	return out, nil
}

func (s *store) CountOpenSessions(ctx context.Context, r editsession.ResourceID, genSeq int64) (int, error) {
	q := s.db.WithContext(ctx).Model(&model.SessionModel{}).
		Where("resource_key = ? AND state = ?", r.Key(), string(editsession.SessionActive))
	if genSeq > 0 {
		q = q.Where("generation = ?", genSeq)
	}
	var n int64
	if err := q.Count(&n).Error; err != nil {
		return 0, fmt.Errorf("editsession: counting sessions: %w", err)
	}
	return int(n), nil
}

func (s *store) CloseSession(ctx context.Context, id string, at time.Time) error {
	res := s.db.WithContext(ctx).Model(&model.SessionModel{}).
		Where("id = ? AND state <> ?", id, string(editsession.SessionClosed)).
		Updates(map[string]any{"state": string(editsession.SessionClosed), "closed_at": at})
	if res.Error != nil {
		return fmt.Errorf("editsession: closing session: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		// either unknown id or already closed; verify existence so an unknown
		// id is reported rather than silently accepted.
		var n int64
		if err := s.db.WithContext(ctx).Model(&model.SessionModel{}).Where("id = ?", id).Count(&n).Error; err != nil {
			return fmt.Errorf("editsession: checking session: %w", err)
		}
		if n == 0 {
			return errtypes.NotFound("editsession: session: " + id)
		}
	}
	return nil
}

// SaveSession persists the mutable session fields.
func (s *store) SaveSession(ctx context.Context, sess *editsession.Session) error {
	row, err := toSessionModel(sess)
	if err != nil {
		return err
	}
	res := s.db.WithContext(ctx).Model(&model.SessionModel{}).Where("id = ?", sess.ID).Updates(map[string]any{
		"generation":  row.Generation,
		"download_id": row.DownloadID,
		"upload_id":   row.UploadID,
		"state":       row.State,
		"closed_at":   row.ClosedAt,
	})
	if res.Error != nil {
		return fmt.Errorf("editsession: saving session: %w", res.Error)
	}
	if res.RowsAffected != 1 {
		return errtypes.NotFound("editsession: session: " + sess.ID)
	}
	return nil
}

// RecordCallback records the callback. A duplicate (already applied) returns
// isNew=false with no error.
func (s *store) RecordCallback(ctx context.Context, r editsession.ResourceID, sessionID string, seq int64, kind editsession.CallbackKind) (bool, error) {
	row := &model.CallbackModel{
		ResourceKey: r.Key(),
		SessionID:   sessionID,
		Seq:         seq,
		Kind:        string(kind),
	}
	err := s.db.WithContext(ctx).Create(row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return false, nil
		}
		return false, fmt.Errorf("editsession: recording callback: %w", err)
	}
	return true, nil
}

// --- uploads ---

func (s *store) RegisterUpload(ctx context.Context, u *editsession.InFlightUpload) error {
	if err := s.db.WithContext(ctx).Create(toUploadModel(u)).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			return errtypes.AlreadyExists("editsession: upload already exists: " + u.ID)
		}
		return fmt.Errorf("editsession: registering upload: %w", err)
	}
	return nil
}

func (s *store) GetUpload(ctx context.Context, id string) (*editsession.InFlightUpload, error) {
	row := &model.UploadModel{}
	err := s.db.WithContext(ctx).Where("id = ?", id).First(row).Error
	if err != nil {
		return nil, mapQueryError(err, "upload")
	}
	return fromUploadModel(row), nil
}

func (s *store) updateUploadState(ctx context.Context, id, state string, at time.Time) error {
	res := s.db.WithContext(ctx).Model(&model.UploadModel{}).
		Where("id = ? AND state = ?", id, string(editsession.UploadOpen)).
		Updates(map[string]any{"state": state, "ended_at": at})
	if res.Error != nil {
		return fmt.Errorf("editsession: updating upload: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		row := &model.UploadModel{}
		err := s.db.WithContext(ctx).Where("id = ?", id).First(row).Error
		if err != nil {
			return mapQueryError(err, "upload")
		}
		// already in a terminal state: idempotent success.
		if row.State != string(editsession.UploadOpen) {
			return nil
		}
		return errtypes.InternalError("editsession: upload not updated: " + id)
	}
	return nil
}

func (s *store) CompleteUpload(ctx context.Context, id string, at time.Time) error {
	return s.updateUploadState(ctx, id, string(editsession.UploadCompleted), at)
}

func (s *store) AbortUpload(ctx context.Context, id string, at time.Time) error {
	return s.updateUploadState(ctx, id, string(editsession.UploadAborted), at)
}

func (s *store) ListOpenUploads(ctx context.Context, r editsession.ResourceID, genSeq int64) ([]*editsession.InFlightUpload, error) {
	q := s.db.WithContext(ctx).Model(&model.UploadModel{}).
		Where("resource_key = ? AND state = ?", r.Key(), string(editsession.UploadOpen))
	if genSeq > 0 {
		q = q.Where("generation = ?", genSeq)
	}
	rows := []*model.UploadModel{}
	if err := q.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("editsession: listing uploads: %w", err)
	}
	out := make([]*editsession.InFlightUpload, 0, len(rows))
	for _, row := range rows {
		out = append(out, fromUploadModel(row))
	}
	return out, nil
}

func mapQueryError(err error, what string) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errtypes.NotFound("editsession: " + what)
	}
	return fmt.Errorf("editsession: querying %s: %w", what, err)
}
