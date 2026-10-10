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

// Package sql implements the rjobs.StatusStore on top of a relational
// database via GORM. It keeps one row per run in the job_runs table, updated
// as the run moves through its lifecycle, and serves it back by run id.
package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cs3org/reva/v3/cmd/revad/pkg/config"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/rjobs"
	"github.com/cs3org/reva/v3/pkg/rjobs/store/sql/model"
	"github.com/pkg/errors"
	"gorm.io/datatypes"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type store struct {
	db *gorm.DB
}

// New opens the database, migrates the schema and returns a status store.
func New(ctx context.Context, c config.Database) (rjobs.StatusStore, error) {
	db, err := openDB(c)
	if err != nil {
		return nil, errors.Wrap(err, "rjobs sql: opening database failed")
	}
	if err := db.AutoMigrate(&model.Run{}); err != nil {
		return nil, errors.Wrap(err, "rjobs sql: migrating schema failed")
	}
	return &store{db: db}, nil
}

func openDB(c config.Database) (*gorm.DB, error) {
	gormCfg := &gorm.Config{}
	switch c.Engine {
	case "sqlite":
		return gorm.Open(sqlite.Open(c.DBName), gormCfg)
	default: // mysql
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		return gorm.Open(mysql.Open(dsn), gormCfg)
	}
}

func (s *store) Put(ctx context.Context, st rjobs.Status) error {
	row, err := toModel(st)
	if err != nil {
		return err
	}
	// later transition. The reservation column is owned by Reserve/Release, the
	// enqueue phase marker by Reserve/MarkCommitted/Revoke and the cancel intent
	// by RequestCancel, so all three are omitted here: a lifecycle write must
	// never wipe a live reservation, regress the commit phase or clobber a
	// concurrent cancel.
	res := s.db.WithContext(ctx).Omit("ActiveDedupKey", "PublishPendingAt", "CancelRequested").Save(row)
	if res.Error != nil {
		return errors.Wrap(res.Error, "rjobs sql: storing status failed")
	}
	return nil
}

func (s *store) RequestCancel(ctx context.Context, id rjobs.RunID) (rjobs.Status, error) {
	// Flip the cancel intent with a targeted update that leaves the columns the
	// worker owns (lifecycle state, timestamps, result) untouched. The state is
	// advanced to cancelling only from a non-terminal state, so a cancel can
	// neither resurrect a finished run nor undo a terminal state.
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND state IN ?", string(id), []string{
			string(rjobs.StateQueued), string(rjobs.StateRunning), string(rjobs.StateFailed),
		}).
		Updates(map[string]any{
			"cancel_requested": true,
			"state":            string(rjobs.StateCancelling),
		})
	if res.Error != nil {
		return rjobs.Status{}, errors.Wrap(res.Error, "rjobs sql: requesting cancel failed")
	}
	// Return the current status whether or not a row was updated: a terminal run
	// is returned unchanged (idempotent cancel) and an unknown run yields
	// NotFound from Get.
	return s.Get(ctx, id)
}

func (s *store) Get(ctx context.Context, id rjobs.RunID) (rjobs.Status, error) {
	var row model.Run
	res := s.db.WithContext(ctx).First(&row, "run_id = ?", string(id))
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return rjobs.Status{}, errtypes.NotFound(fmt.Sprintf("run %q not found", id))
		}
		return rjobs.Status{}, errors.Wrap(res.Error, "rjobs sql: reading status failed")
	}
	return fromModel(row)
}

func (s *store) List(ctx context.Context, f rjobs.ListFilter) ([]rjobs.Status, error) {
	q := s.db.WithContext(ctx).Model(&model.Run{})
	switch {
	case f.Internal:
		q = q.Where("owner = ?", "")
	case f.Owner != "":
		q = q.Where("owner = ?", f.Owner)
	}
	if len(f.States) > 0 {
		states := make([]string, len(f.States))
		for i, st := range f.States {
			states[i] = string(st)
		}
		q = q.Where("state IN ?", states)
	}
	if f.Job != "" {
		q = q.Where("job = ?", f.Job)
	}
	q = q.Order("enqueued_at DESC")
	if f.Limit > 0 {
		q = q.Limit(f.Limit)
	}
	if f.Offset > 0 {
		q = q.Offset(f.Offset)
	}

	var rows []model.Run
	if err := q.Find(&rows).Error; err != nil {
		return nil, errors.Wrap(err, "rjobs sql: listing runs failed")
	}
	out := make([]rjobs.Status, 0, len(rows))
	for _, row := range rows {
		st, err := fromModel(row)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

func (s *store) Reserve(ctx context.Context, st rjobs.Status, key string) (rjobs.Status, bool, error) {
	// Retry to cover the narrow window where the current holder releases the key
	// between our insert seeing it taken and the lookup of that holder.
	for range 5 {
		row, err := toModel(st)
		if err != nil {
			return rjobs.Status{}, false, err
		}
		row.ActiveDedupKey = &key
		// enter the publish-pending commit phase: the placeholder exists from
		// this insert until MarkCommitted confirms the queue publish.
		pendingAt := time.Now()
		row.PublishPendingAt = &pendingAt

		// Insert unless (owner, key) is already taken: a unique-index conflict is
		// the expected "key already held" case, so the database skips it instead
		// of erroring. RowsAffected == 1 means our row went in — we won the key.
		res := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(row)
		if res.Error != nil {
			return rjobs.Status{}, false, errors.Wrap(res.Error, "rjobs sql: reserving run failed")
		}
		if res.RowsAffected == 1 {
			return rjobs.Status{}, true, nil
		}

		holder, found, err := s.activeHolder(ctx, st.Owner, key)
		if err != nil {
			return rjobs.Status{}, false, err
		}
		if found {
			return holder, false, nil
		}
		// the holder released the key just now; loop and try to take it.
	}
	return rjobs.Status{}, false, errors.New("rjobs sql: could not reserve run, key is contended")
}

func (s *store) Release(ctx context.Context, id rjobs.RunID) error {
	// gorm skips a bare nil in a single-column Update, so spell out NULL.
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ?", string(id)).
		Update("active_dedup_key", gorm.Expr("NULL"))
	if res.Error != nil {
		return errors.Wrap(res.Error, "rjobs sql: releasing reservation failed")
	}
	return nil
}

func (s *store) MarkCommitted(ctx context.Context, id rjobs.RunID) error {
	// Only a still-pending queued row leaves the phase. A row revoked in the
	// meantime is StateFailed with the marker left set on purpose, so its
	// late-arriving message is recognised as obsolete.
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND state = ? AND publish_pending_at IS NOT NULL",
			string(id), string(rjobs.StateQueued)).
		Update("publish_pending_at", gorm.Expr("NULL"))
	if res.Error != nil {
		return errors.Wrap(res.Error, "rjobs sql: marking enqueue committed failed")
	}
	return nil
}

// revokeUpdate is the write that abandons a reservation: the run becomes a
// failed record for observability and its dedup key is freed. The pending
// marker is deliberately not part of the update, so it stays set and a message
// published just before the crash is dropped at adoption instead of executing.
func (s *store) revokeUpdate(q *gorm.DB, reason string) error {
	now := time.Now()
	return q.Updates(map[string]any{
		"state":            string(rjobs.StateFailed),
		"active_dedup_key": nil,
		"finished_at":      now,
		"last_error":       reason,
	}).Error
}

func (s *store) Revoke(ctx context.Context, id rjobs.RunID, reason string) error {
	q := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND state = ? AND publish_pending_at IS NOT NULL",
			string(id), string(rjobs.StateQueued))
	if err := s.revokeUpdate(q, reason); err != nil {
		return errors.Wrap(err, "rjobs sql: revoking reservation failed")
	}
	return nil
}

func (s *store) RevokePending(ctx context.Context, grace time.Duration) ([]rjobs.RunID, error) {
	cutoff := time.Now().Add(-grace)

	var ids []string
	if err := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("state = ? AND publish_pending_at IS NOT NULL AND publish_pending_at < ?",
			string(rjobs.StateQueued), cutoff).
		Pluck("run_id", &ids).Error; err != nil {
		return nil, errors.Wrap(err, "rjobs sql: listing pending reservations failed")
	}
	if len(ids) == 0 {
		return nil, nil
	}

	q := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id IN ?", ids)
	if err := s.revokeUpdate(q, "rjobs: enqueue interrupted before the run was published"); err != nil {
		return nil, errors.Wrap(err, "rjobs sql: revoking pending reservations failed")
	}

	out := make([]rjobs.RunID, 0, len(ids))
	for _, id := range ids {
		out = append(out, rjobs.RunID(id))
	}
	return out, nil
}

func (s *store) Adopt(ctx context.Context, skeleton rjobs.Status, attempt int) (rjobs.AdoptDecision, rjobs.Status, error) {
	if attempt <= 0 {
		attempt = 1
	}
	id := string(skeleton.RunID)

	// A few rows may move under concurrent deliveries; re-read and re-decide
	// when a conditioned update loses a race.
	for range 3 {
		var row model.Run
		err := s.db.WithContext(ctx).First(&row, "run_id = ?", id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// No status row: the message reached the queue but the state write
			// never did. Rebuild the ORIGINAL run's state from the identity
			// carried by the message, so it is queryable instead of executing
			// unknown.
			now := time.Now()
			row = model.Run{
				RunID:      id,
				Job:        skeleton.Job,
				State:      string(rjobs.StateRunning),
				Attempt:    attempt,
				EnqueuedAt: skeleton.EnqueuedAt,
				StartedAt:  &now,
				Owner:      skeleton.Owner,
			}
			res := s.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
			if res.Error != nil {
				return 0, rjobs.Status{}, errors.Wrap(res.Error, "rjobs sql: rebuilding missing run state failed")
			}
			if res.RowsAffected == 1 {
				st, err := fromModel(row)
				return rjobs.AdoptExecute, st, err
			}
			// a concurrent adoption created the row first; re-read and decide.
			continue
		}
		if err != nil {
			return 0, rjobs.Status{}, errors.Wrap(err, "rjobs sql: reading run for adoption failed")
		}

		state := rjobs.State(row.State)
		switch {
		case state == rjobs.StateSucceeded || state == rjobs.StateCancelled:
			// already terminal: never run, never resurrect.
			st, err := fromModel(row)
			return rjobs.AdoptObsolete, st, err
		case state == rjobs.StateFailed && row.PublishPendingAt != nil:
			// a revoked placeholder: the publication was abandoned. Any message
			// that still arrives for it is obsolete.
			st, err := fromModel(row)
			return rjobs.AdoptObsolete, st, err
		case row.Attempt > attempt:
			// this delivery belongs to an older attempt; a newer attempt owns
			// the run now. The old owner must stand down.
			st, err := fromModel(row)
			return rjobs.AdoptObsolete, st, err
		case row.Attempt == attempt && state == rjobs.StateRunning:
			// a duplicate delivery of the attempt already being worked.
			st, err := fromModel(row)
			return rjobs.AdoptObsolete, st, err
		}

		// Take ownership: this delivery (possibly a redelivery) is the current
		// attempt now.
		resubmit := len(row.Result) > 0
		nextState := string(rjobs.StateRunning)
		if row.CancelRequested {
			// a cancel landed while queued: keep winding down instead of
			// flipping back to running.
			nextState = string(rjobs.StateCancelling)
		}
		updates := map[string]any{
			"attempt":            attempt,
			"state":              nextState,
			"finished_at":        nil,
			"last_error":         "",
			"publish_pending_at": nil, // the message physically exists: the enqueue is committed
		}
		if !resubmit {
			now := time.Now()
			updates["started_at"] = now
		}
		res := s.db.WithContext(ctx).Model(&model.Run{}).
			Where("run_id = ? AND attempt = ?", id, row.Attempt).
			Updates(updates)
		if res.Error != nil {
			return 0, rjobs.Status{}, errors.Wrap(res.Error, "rjobs sql: taking ownership of run failed")
		}
		if res.RowsAffected == 0 {
			// a concurrent delivery moved the row first; re-decide.
			continue
		}

		var updated model.Run
		if err := s.db.WithContext(ctx).First(&updated, "run_id = ?", id).Error; err != nil {
			return 0, rjobs.Status{}, errors.Wrap(err, "rjobs sql: reading adopted run failed")
		}
		st, err := fromModel(updated)
		if err != nil {
			return 0, rjobs.Status{}, err
		}
		if resubmit {
			return rjobs.AdoptResubmit, st, nil
		}
		return rjobs.AdoptExecute, st, nil
	}

	return 0, rjobs.Status{}, errors.New("rjobs sql: run adoption contended repeatedly")
}

func (s *store) StageResult(ctx context.Context, id rjobs.RunID, attempt int, result rjobs.Params) error {
	payload, err := marshalResult(result)
	if err != nil {
		return err
	}
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND attempt = ? AND state IN ?",
			string(id), attempt, []string{string(rjobs.StateRunning), string(rjobs.StateCancelling)}).
		Update("result", payload)
	if res.Error != nil {
		return errors.Wrap(res.Error, "rjobs sql: staging run result failed")
	}
	if res.RowsAffected == 0 {
		return errors.Errorf("rjobs sql: cannot stage result, run %q attempt %d no longer owns the run", id, attempt)
	}
	return nil
}

func (s *store) MarkFailed(ctx context.Context, id rjobs.RunID, attempt int, lastError string, at time.Time) (bool, error) {
	// A durable cancel intent (StateCancelling) wins over the failure, like the
	// worker's in-process cancel flag: the run is left for the next delivery to
	// finalise as cancelled instead of being retried as failed.
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND attempt = ? AND state NOT IN ?",
			string(id), attempt,
			[]string{string(rjobs.StateSucceeded), string(rjobs.StateCancelled), string(rjobs.StateCancelling)}).
		Updates(map[string]any{
			"state":       string(rjobs.StateFailed),
			"finished_at": at,
			"last_error":  lastError,
		})
	if res.Error != nil {
		return false, errors.Wrap(res.Error, "rjobs sql: recording failed attempt failed")
	}
	return res.RowsAffected == 1, nil
}

func (s *store) CommitTerminal(ctx context.Context, st rjobs.Status) (bool, error) {
	payload, err := marshalResult(st.Result)
	if err != nil {
		return false, err
	}
	finished := time.Now()
	if st.FinishedAt != nil {
		finished = *st.FinishedAt
	}

	// A cancel commit may leave StateRunning (a cancel that reached the worker
	// only through the broadcast); a success commit may not win once a durable
	// cancel intent has recorded StateCancelling — the redelivery finalises the
	// run as cancelled. Neither write can touch an already terminal row.
	excluded := []string{string(rjobs.StateSucceeded), string(rjobs.StateCancelled)}
	if st.State == rjobs.StateSucceeded {
		excluded = append(excluded, string(rjobs.StateCancelling))
	}
	res := s.db.WithContext(ctx).Model(&model.Run{}).
		Where("run_id = ? AND attempt = ? AND state NOT IN ?",
			string(st.RunID), st.Attempt, excluded).
		Updates(map[string]any{
			"state":       string(st.State),
			"started_at":  st.StartedAt,
			"finished_at": finished,
			"last_error":  st.LastError,
			"result":      payload,
		})
	if res.Error != nil {
		return false, errors.Wrap(res.Error, "rjobs sql: committing terminal state failed")
	}
	return res.RowsAffected == 1, nil
}

// activeHolder loads the run currently holding key for owner, if any.
func (s *store) activeHolder(ctx context.Context, owner, key string) (rjobs.Status, bool, error) {
	q := s.db.WithContext(ctx).Where("owner = ? AND active_dedup_key = ?", owner, key)
	var row model.Run
	if err := q.First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return rjobs.Status{}, false, nil
		}
		return rjobs.Status{}, false, errors.Wrap(err, "rjobs sql: loading reservation holder failed")
	}
	st, err := fromModel(row)
	if err != nil {
		return rjobs.Status{}, false, err
	}
	return st, true, nil
}

func (s *store) Close(ctx context.Context) error {
	db, err := s.db.DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// marshalResult encodes a run result for the JSON column. A nil result is a
// SQL NULL, distinct from an empty JSON object.
func marshalResult(st rjobs.Params) (datatypes.JSON, error) {
	if st == nil {
		return nil, nil
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil, errors.Wrap(err, "rjobs sql: marshalling result failed")
	}
	return b, nil
}

func toModel(st rjobs.Status) (*model.Run, error) {
	result, err := marshalResult(st.Result)
	if err != nil {
		return nil, err
	}
	m := &model.Run{
		RunID:           string(st.RunID),
		Job:             st.Job,
		State:           string(st.State),
		Attempt:         st.Attempt,
		EnqueuedAt:      st.EnqueuedAt,
		StartedAt:       st.StartedAt,
		FinishedAt:      st.FinishedAt,
		LastError:       st.LastError,
		Result:          result,
		CancelRequested: st.CancelRequested,
	}
	m.Owner = st.Owner
	return m, nil
}

func fromModel(row model.Run) (rjobs.Status, error) {
	st := rjobs.Status{
		RunID:           rjobs.RunID(row.RunID),
		Job:             row.Job,
		State:           rjobs.State(row.State),
		Attempt:         row.Attempt,
		EnqueuedAt:      row.EnqueuedAt,
		StartedAt:       row.StartedAt,
		FinishedAt:      row.FinishedAt,
		LastError:       row.LastError,
		CancelRequested: row.CancelRequested,
	}
	st.Owner = row.Owner
	if len(row.Result) > 0 {
		var p rjobs.Params
		if err := json.Unmarshal(row.Result, &p); err != nil {
			return rjobs.Status{}, errors.Wrap(err, "rjobs sql: unmarshalling result failed")
		}
		st.Result = p
	}
	return st, nil
}
