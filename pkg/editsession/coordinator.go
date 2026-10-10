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
	"errors"
	"sync"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/cs3org/reva/v3/pkg/errtypes"
)

// Config tunes the coordinator.
type Config struct {
	// DrainTimeout bounds the wait for in-flight saves before the entry
	// switch.
	DrainTimeout time.Duration
	// DrainPoll is the interval between drain probes.
	DrainPoll time.Duration
}

func (c *Config) applyDefaults() {
	if c.DrainTimeout == 0 {
		c.DrainTimeout = 10 * time.Second
	}
	if c.DrainPoll == 0 {
		c.DrainPoll = 20 * time.Millisecond
	}
}

// Coordinator drives editing generations through their state machine.
type Coordinator struct {
	store   Store
	storage Storage
	locker  Locker
	app     AppProvider
	ends    EndpointIssuer
	clock   Clock
	cfg     Config

	// singleflight: at most one goroutine drives a resource's generation;
	// concurrent moves/joins wait for it.
	runMu sync.Mutex
	runs  map[string]*driveRun
}

type driveRun struct {
	done chan struct{}
	g    *Generation
	err  error
}

// New builds a Coordinator.
func New(st Store, storage Storage, locker Locker, app AppProvider, ends EndpointIssuer, conf ...Config) *Coordinator {
	c := &Coordinator{
		store:   st,
		storage: storage,
		locker:  locker,
		app:     app,
		ends:    ends,
		clock:   SystemClock{},
	}
	if len(conf) > 0 {
		c.cfg = conf[0]
	}
	c.cfg.applyDefaults()
	c.runs = map[string]*driveRun{}
	return c
}

// runOrJoin drives g itself or waits for an in-flight drive of the same
// resource. Ensures concurrent moves never run the protocol twice and can
// never mask a failure with a parallel drive.
func (c *Coordinator) runOrJoin(ctx context.Context, g *Generation) (*Generation, error) {
	key := g.Resource.Key()
	c.runMu.Lock()
	if run, ok := c.runs[key]; ok {
		c.runMu.Unlock()
		select {
		case <-run.done:
			return run.g, run.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	run := &driveRun{done: make(chan struct{})}
	c.runs[key] = run
	c.runMu.Unlock()

	run.g, run.err = c.drive(ctx, g)
	// always return the persisted state so callers never observe a stale
	// in-memory generation (e.g. an aborted gen whose pointer still says
	// moving).
	if latest, e := c.store.GetGeneration(ctx, g.Resource, g.Sequence); e == nil {
		run.g = latest
	}
	close(run.done)

	c.runMu.Lock()
	delete(c.runs, key)
	c.runMu.Unlock()
	return run.g, run.err
}

// Store returns the persistent store.
// NewCommitGate builds a coordinator that only serves the commit gate
// (CommitAllowed): it needs the store but none of the drive ports, which is
// what a data gateway uses to validate saves before proxying them.
func NewCommitGate(store Store) *Coordinator {
	c := &Coordinator{store: store, clock: SystemClock{}}
	c.cfg.applyDefaults()
	return c
}

func (c *Coordinator) Store() Store { return c.store }

// --- open / freeze gates ---

// CheckEditOpen rejects new edit opens while a generation is in progress.
// Read-only opens never call this gate.
func (c *Coordinator) CheckEditOpen(ctx context.Context, r ResourceID) error {
	g, err := c.store.GetInProgressGeneration(ctx, r)
	if err != nil {
		return err
	}
	if g != nil {
		return wrap(ErrFrozen, "generation %d is %s", g.Sequence, g.State)
	}
	return nil
}

// SetLockHint lets the wiring correlate the session with a lock already
// held for the resource (apps take the write lock before/around open).
//
// OpenSession registers a session after a successful read-write open,
// creating the base generation on first use.
func (c *Coordinator) OpenSession(ctx context.Context, sess *Session) error {
	if err := c.CheckEditOpen(ctx, sess.Resource); err != nil {
		return err
	}
	gens, err := c.store.ListGenerations(ctx, sess.Resource)
	if err != nil {
		return err
	}
	seq := int64(1)
	if prev := servingGeneration(gens); prev != nil {
		seq = prev.Sequence
	} else if len(gens) > 0 {
		// only aborted generations exist: create a fresh base generation.
		seq = gens[len(gens)-1].Sequence + 1
		now := c.clock.Now()
		g := &Generation{
			Sequence: seq, Resource: sess.Resource, State: GenActive,
			Confirmations: map[Party]bool{}, CreatedAt: now, UpdatedAt: now,
		}
		if err := c.store.CreateGeneration(ctx, g); err != nil {
			return err
		}
	} else {
		now := c.clock.Now()
		g := &Generation{
			Sequence: 1, Resource: sess.Resource, State: GenActive,
			Confirmations: map[Party]bool{}, CreatedAt: now, UpdatedAt: now,
		}
		if err := c.store.CreateGeneration(ctx, g); err != nil {
			return err
		}
	}

	if sess.State == "" {
		sess.State = SessionActive
	}
	sess.Generation = seq
	sess.OpenedAt = c.clock.Now()
	return c.store.CreateSession(ctx, sess)
}

// servingGeneration returns the non-aborted generation with the greatest
// sequence, or nil when every generation was aborted / none exists.
func servingGeneration(gens []*Generation) *Generation {
	for i := len(gens) - 1; i >= 0; i-- {
		if gens[i].State != GenAborted {
			return gens[i]
		}
	}
	return nil
}

// --- in-flight saves ---

// BeginUpload registers a save initiated through InitiateFileUpload, binding
// it to the currently serving generation. New saves are rejected while a
// generation move is in progress.
func (c *Coordinator) BeginUpload(ctx context.Context, u *InFlightUpload) error {
	if err := c.CheckEditOpen(ctx, u.Resource); err != nil {
		return err
	}
	gens, err := c.store.ListGenerations(ctx, u.Resource)
	if err != nil {
		return err
	}
	prev := servingGeneration(gens)
	if prev == nil {
		return wrap(ErrNoSessions, "resource has no serving generation")
	}
	u.Generation = prev.Sequence
	u.State = UploadOpen
	u.StartedAt = c.clock.Now()
	return c.store.RegisterUpload(ctx, u)
}

// CommitAllowed is the gate a data gateway invokes before letting a save
// commit. It encodes the rule: saves are allowed on the serving generation,
// on a generation during its drain window, and never once the new reference
// committed (old tokens must not overwrite).
func (c *Coordinator) CommitAllowed(ctx context.Context, uploadID string) error {
	u, err := c.store.GetUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	if u.State != UploadOpen {
		return wrap(ErrGenerationMismatch, "upload %s is not open (state %s)", uploadID, u.State)
	}
	ok, err := c.generationCommittable(ctx, u.Resource, u.Generation)
	if err != nil {
		return err
	}
	if !ok {
		return wrap(ErrGenerationMismatch, "upload %s bound to generation %d", uploadID, u.Generation)
	}
	return nil
}

// CompleteUpload marks a save completed.
func (c *Coordinator) CompleteUpload(ctx context.Context, uploadID string) error {
	return c.store.CompleteUpload(ctx, uploadID, c.clock.Now())
}

// AbortUpload marks a save aborted.
func (c *Coordinator) AbortUpload(ctx context.Context, uploadID string) error {
	return c.store.AbortUpload(ctx, uploadID, c.clock.Now())
}

// generationCommittable reports whether saves bound to generation seq may
// commit now.
func (c *Coordinator) generationCommittable(ctx context.Context, r ResourceID, seq int64) (bool, error) {
	g, err := c.store.GetGeneration(ctx, r, seq)
	if err != nil {
		return false, err
	}
	switch g.State {
	case GenFreezing:
		// upload bound to the not-yet-switched generation: harmless allow.
		return true, nil
	case GenMoving:
		// entry already switched: old saves must not land.
		return false, nil
	case GenAborted:
		return false, nil
	}
	// active / confirmed: it is committable while unchallenged, during the
	// successor's drain window, or after the successor aborted (serving
	// restored). It is closed once the successor moved.
	if g.SupersededBy == 0 {
		return true, nil
	}
	next, err := c.store.GetGeneration(ctx, r, g.SupersededBy)
	if err != nil {
		return false, err
	}
	switch next.State {
	case GenFreezing, GenAborted:
		return true, nil
	default:
		return false, nil
	}
}

// --- move ---

// Move runs the generation move: it resumes an in-progress generation when
// one exists, otherwise opens a new generation and drives it. A move without
// open sessions fails and the caller must take the plain move path.
func (c *Coordinator) Move(ctx context.Context, r ResourceID, oldRef, newRef *provider.Reference) (*Generation, error) {
	if g, err := c.store.GetInProgressGeneration(ctx, r); err != nil {
		return nil, err
	} else if g != nil {
		log := c.log(ctx, r)
		log.Info().Int64("generation", g.Sequence).Str("state", string(g.State)).
			Msg("resuming in-progress generation")
		return c.runOrJoin(ctx, g)
	}

	gens, err := c.store.ListGenerations(ctx, r)
	if err != nil {
		return nil, err
	}
	if len(gens) == 0 {
		return nil, wrap(ErrNoSessions, "no serving generation for %s", r.Key())
	}
	// A concurrent move may have created a new generation after the
	// in-progress check above. If the latest generation is unfinished it
	// belongs to that move: join it instead of opening a competing one.
	if latest := gens[len(gens)-1]; latest.State == GenFreezing || latest.State == GenMoving {
		log := c.log(ctx, r)
		log.Info().Int64("generation", latest.Sequence).Str("state", string(latest.State)).
			Msg("joining concurrently opened generation")
		return c.runOrJoin(ctx, latest)
	}

	prev := servingGeneration(gens)
	if prev == nil {
		return nil, wrap(ErrNoSessions, "no serving generation for %s", r.Key())
	}
	n, err := c.store.CountOpenSessions(ctx, r, prev.Sequence)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// Another move may have fully completed between the check above
		// and this count (its sessions are no longer on prev). Re-check
		// once more and join rather than reporting no sessions.
		if ing, ierr := c.store.GetInProgressGeneration(ctx, r); ierr == nil && ing != nil {
			return c.runOrJoin(ctx, ing)
		}
		return nil, wrap(ErrNoSessions, "resource %s", r.Key())
	}

	newSeq := gens[len(gens)-1].Sequence + 1
	now := c.clock.Now()
	g := &Generation{
		Sequence: newSeq, Resource: r,
		OldPath: oldRef.GetPath(), NewPath: newRef.GetPath(),
		State: GenFreezing, Confirmations: map[Party]bool{},
		CreatedAt: now, UpdatedAt: now,
	}
	if err := c.store.CreateGeneration(ctx, g); err != nil {
		// concurrent cold move: another request won the CreateGeneration
		// race; join that generation instead of surfacing an internal error.
		var exists errtypes.AlreadyExists
		if errors.As(err, &exists) {
			if ing, ierr := c.store.GetInProgressGeneration(ctx, r); ierr == nil && ing != nil {
				return c.runOrJoin(ctx, ing)
			}
		}
		return nil, err
	}
	log := c.log(ctx, r)
	log.Info().Int64("generation", newSeq).Msg("opened new editing generation")
	return c.runOrJoin(ctx, g)
}

func (c *Coordinator) drive(ctx context.Context, g *Generation) (*Generation, error) {
	var err error
	if g.State == GenFreezing {
		if err = c.freezeAndSwitch(ctx, g); err != nil {
			return g, err
		}
		g, err = c.store.GetGeneration(ctx, g.Resource, g.Sequence)
		if err != nil {
			return nil, err
		}
		if g.State != GenMoving {
			return g, nil // aborted during switch
		}
	}
	if g.State == GenMoving {
		if err = c.takeOver(ctx, g); err != nil {
			return g, err
		}
		g, err = c.store.GetGeneration(ctx, g.Resource, g.Sequence)
		if err != nil {
			return nil, err
		}
		return g, nil
	}
	return g, nil
}

// predecessorSeq finds the generation succeeded by g (the one carrying the
// SupersededBy marker), 0 when it cannot be found.
func (c *Coordinator) predecessorSeq(ctx context.Context, g *Generation) (int64, error) {
	gens, err := c.store.ListGenerations(ctx, g.Resource)
	if err != nil {
		return 0, err
	}
	for _, x := range gens {
		if x.SupersededBy == g.Sequence {
			return x.Sequence, nil
		}
	}
	return 0, nil
}

// freezeAndSwitch drains in-flight saves of the predecessor, switches the
// directory entry and persists the moving phase.
func (c *Coordinator) freezeAndSwitch(ctx context.Context, g *Generation) error {
	log := c.log(ctx, g.Resource)
	prevSeq, err := c.predecessorSeq(ctx, g)
	if err != nil {
		return err
	}
	if prevSeq == 0 {
		// crash window before the marker was written: reconstruct it from the
		// generation order and self-heal. The predecessor is the greatest
		// non-aborted generation strictly below g (g itself is already
		// persisted and must not be picked).
		gens, err := c.store.ListGenerations(ctx, g.Resource)
		if err != nil {
			return err
		}
		var prev *Generation
		for _, x := range gens {
			if x.State != GenAborted && x.Sequence < g.Sequence {
				prev = x
			}
		}
		if prev != nil {
			prevSeq = prev.Sequence
			if err := c.store.MarkSuperseded(ctx, g.Resource, prevSeq, g.Sequence); err != nil {
				return err
			}
		}
	}

	// drain in-flight saves bound to the predecessor.
	deadline := c.clock.Now().Add(c.cfg.DrainTimeout)
	for {
		open, err := c.store.ListOpenUploads(ctx, g.Resource, prevSeq)
		if err != nil {
			return err
		}
		if len(open) == 0 {
			break
		}
		if c.clock.Now().After(deadline) {
			log.Warn().Int("open_uploads", len(open)).Msg("drain timeout, aborting generation")
			return c.abort(ctx, g, errors.New("drain timeout"))
		}
		if !sleep(ctx, c.cfg.DrainPoll) {
			return context.Canceled
		}
	}

	oldRef := &provider.Reference{Path: g.OldPath, ResourceId: g.Resource.ToCS3()}
	newRef := &provider.Reference{Path: g.NewPath, ResourceId: g.Resource.ToCS3()}

	if err := c.storage.Move(ctx, oldRef, newRef); err != nil {
		if errors.Is(err, ErrAlreadyMoved) {
			log.Debug().Msg("entry already at destination")
		} else {
			// ambiguous: probe destination; only continue when the entry with
			// our identity is present there.
			info, statErr := c.storage.Stat(ctx, newRef)
			if statErr != nil || info.GetId().GetOpaqueId() != g.Resource.OpaqueID {
				return c.abort(ctx, g, err)
			}
		}
	}

	g, err = c.store.TransitionGeneration(ctx, g.Resource, g.Sequence,
		[]GenState{GenFreezing}, GenMoving)
	if err != nil {
		return err
	}
	log.Info().Msg("directory entry switched, taking over")
	return nil
}

// takeOver drives the moving-phase confirmations.
func (c *Coordinator) takeOver(ctx context.Context, g *Generation) error {
	if err := c.confirmSessions(ctx, g); err != nil {
		return err
	}
	if err := c.confirmLock(ctx, g); err != nil {
		return err
	}
	if err := c.confirmEndpoints(ctx, g); err != nil {
		return err
	}

	cur, err := c.store.GetGeneration(ctx, g.Resource, g.Sequence)
	if err != nil {
		return err
	}
	if cur.AllConfirmed() {
		if _, err := c.store.TransitionGeneration(ctx, g.Resource, g.Sequence,
			[]GenState{GenMoving}, GenConfirmed); err != nil {
			return err
		}
		log := c.log(ctx, g.Resource)
		log.Info().Msg("generation confirmed")
	}
	return nil
}

func (c *Coordinator) newRef(g *Generation) *provider.Reference {
	return &provider.Reference{Path: g.NewPath, ResourceId: g.Resource.ToCS3()}
}

// confirmSessions relocates every open session and records the sessions +
// app-provider confirmations.
func (c *Coordinator) confirmSessions(ctx context.Context, g *Generation) error {
	if !g.Confirmed(PartySessions) {
		prevSeq, err := c.predecessorSeq(ctx, g)
		if err != nil {
			return err
		}
		// sessions may sit at the predecessor (not yet moved) or already at
		// the new generation (moved but party not persisted).
		candidates := map[string]*Session{}
		if prevSeq > 0 {
			sessions, err := c.store.ListSessions(ctx, g.Resource, prevSeq, true)
			if err != nil {
				return err
			}
			for _, s := range sessions {
				candidates[s.ID] = s
			}
		}
		moved, err := c.store.ListSessions(ctx, g.Resource, g.Sequence, true)
		if err != nil {
			return err
		}
		for _, s := range moved {
			candidates[s.ID] = s
		}

		for _, s := range candidates {
			if err := c.relocateOne(ctx, g, s); err != nil {
				return err
			}
		}
		if err := c.store.RecordConfirmation(ctx, g.Resource, g.Sequence, PartySessions); err != nil {
			return err
		}
	}

	if !g.Confirmed(PartyAppProvider) {
		if err := c.app.ConfirmReference(ctx, g.Resource, c.newRef(g), g.Sequence); err != nil {
			return err
		}
		return c.store.RecordConfirmation(ctx, g.Resource, g.Sequence, PartyAppProvider)
	}
	return nil
}

// relocateOne moves one session, reconciling an ambiguous callback timeout
// via QuerySaved.
func (c *Coordinator) relocateOne(ctx context.Context, g *Generation, s *Session) error {
	if s.Generation == g.Sequence {
		return nil
	}
	err := c.app.TakeOver(ctx, s.ID, g.Resource, c.newRef(g), g.Sequence)
	if err != nil {
		if !errors.Is(err, ErrAppCallbackTimeout) {
			return err
		}
		saved, savedGen, qerr := c.app.QuerySaved(ctx, s.ID, g.Resource)
		if qerr != nil {
			return qerr
		}
		if !saved || savedGen < g.Sequence {
			// unknown / not done: keep the generation resumable, do not mask.
			return wrap(err, "session %s takeover unverified", s.ID)
		}
	}
	// re-read before writing: a session closed while the takeover call was
	// in flight must not be resurrected by our snapshot.
	fresh, err := c.store.GetSession(ctx, s.ID)
	if err != nil {
		return err
	}
	if !fresh.IsOpen() {
		return nil
	}
	fresh.Generation = g.Sequence
	return c.store.SaveSession(ctx, fresh)
}

// confirmLock confirms the write lock at the new reference before the old one
// is released. A lock failure aborts the move: the original location is
// preserved and service restored.
func (c *Coordinator) confirmLock(ctx context.Context, g *Generation) error {
	if g.Confirmed(PartyLock) {
		return nil
	}
	prevSeq, err := c.predecessorSeq(ctx, g)
	if err != nil {
		return err
	}

	var lockID string
	sessions, err := c.store.ListSessions(ctx, g.Resource, 0, true)
	if err != nil {
		return err
	}
	for _, s := range sessions {
		if s.LockID != "" {
			lockID = s.LockID
			break
		}
	}
	lock := &provider.Lock{LockId: lockID}
	newRef := c.newRef(g)
	oldRef := &provider.Reference{Path: g.OldPath, ResourceId: g.Resource.ToCS3()}

	cur, err := c.locker.GetLock(ctx, newRef)
	switch {
	case err == nil:
		if cur.GetLockId() != lockID {
			return c.abort(ctx, g, errors.New("lock at new reference has a different holder"))
		}
		if err := c.locker.RefreshLock(ctx, newRef, lock, cur.GetLockId()); err != nil {
			return c.abort(ctx, g, err)
		}
	case isNotFound(err):
		if err := c.locker.SetLock(ctx, newRef, lock); err != nil {
			return c.abort(ctx, g, err)
		}
	default:
		return c.abort(ctx, g, err)
	}

	if err := c.store.RecordConfirmation(ctx, g.Resource, g.Sequence, PartyLock); err != nil {
		return err
	}

	// new lock is confirmed: release the old one. When the storage moves the
	// lock with the inode, the old lock is already gone: not-found is fine.
	if err := c.locker.Unlock(ctx, oldRef, lock); err != nil {
		if !isNotFound(err) {
			// old lock unclear; generation can still serve from the new
			// location, surface as a non-fatal warning (a late old unlock may
			// be retried via recovery without affecting content).
			warnLog := c.log(ctx, g.Resource)
			warnLog.Warn().Err(err).Msg("could not release old lock")
		}
	}
	_ = prevSeq
	return nil
}

// notFound matches any errtypes.NotFound value (errors.As over string types).
type notFound interface {
	error
	IsNotFound()
}

// endpoint retry tuning.
const (
	endpointAttempts = 3
	endpointWait     = 5 * time.Millisecond
)

// retry runs fn up to endpointAttempts times, waiting between tries. It
// returns the last error when every attempt fails; ctx cancellation aborts.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := 0; attempt < endpointAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt+1 < endpointAttempts {
			if !sleep(ctx, endpointWait) {
				return context.Canceled
			}
		}
	}
	return err
}

// confirmEndpoints re-issues download/upload endpoints at the new path for
// every open session. A failure after bounded retries surfaces to the Move
// caller (the generation stays moving and is resumable); it is never a
// silent success.
func (c *Coordinator) confirmEndpoints(ctx context.Context, g *Generation) error {
	if !g.Confirmed(PartyDownload) {
		sessions, err := c.store.ListSessions(ctx, g.Resource, g.Sequence, true)
		if err != nil {
			return err
		}
		for _, s := range sessions {
			var id string
			err := retry(ctx, func() error {
				var rerr error
				id, rerr = c.ends.ReissueDownload(ctx, s, c.newRef(g))
				return rerr
			})
			if err != nil {
				return err
			}
			s.DownloadID = id
			if err := c.store.SaveSession(ctx, s); err != nil {
				return err
			}
		}
		if err := c.store.RecordConfirmation(ctx, g.Resource, g.Sequence, PartyDownload); err != nil {
			return err
		}
	}

	if !g.Confirmed(PartyUpload) {
		sessions, err := c.store.ListSessions(ctx, g.Resource, g.Sequence, true)
		if err != nil {
			return err
		}
		for _, s := range sessions {
			var id string
			err := retry(ctx, func() error {
				var rerr error
				id, rerr = c.ends.ReissueUpload(ctx, s, c.newRef(g))
				return rerr
			})
			if err != nil {
				return err
			}
			s.UploadID = id
			if err := c.store.SaveSession(ctx, s); err != nil {
				return err
			}
		}
		return c.store.RecordConfirmation(ctx, g.Resource, g.Sequence, PartyUpload)
	}
	return nil
}

// abort rolls the generation back: moves the entry back when it had switched,
// releases a lock placed at the new reference, marks the generation aborted
// and restores the predecessor's serving marker.
func (c *Coordinator) abort(ctx context.Context, g *Generation, reason error) error {
	log := c.log(ctx, g.Resource)
	oldRef := &provider.Reference{Path: g.OldPath, ResourceId: g.Resource.ToCS3()}
	newRef := c.newRef(g)

	// reconcile the entry wherever it actually is: probe the old path and,
	// when it is absent, the new path. This covers both the moving state
	// (entry definitely moved) and ambiguous failures from freezing (the
	// move may or may not have landed).
	entryAtNew := false
	if _, err := c.storage.Stat(ctx, oldRef); err != nil {
		if info, statErr := c.storage.Stat(ctx, newRef); statErr == nil &&
			info.GetId().GetOpaqueId() == g.Resource.OpaqueID {
			entryAtNew = true
		}
	}
	if entryAtNew {
		if err := c.storage.Move(ctx, newRef, oldRef); err != nil && !errors.Is(err, ErrAlreadyMoved) {
			// cannot roll the entry back here; leave the generation in place
			// for restart recovery rather than masking with a new move.
			log.Error().Err(err).Msg("could not roll entry back; awaiting recovery")
			return err
		}
	}
	// only release a lock that belongs to this move: gather the lock ids
	// the resource's sessions hold; a lock with an unrelated id at the new
	// path is someone else's and must be left alone.
	expectedLocks := map[string]bool{}
	openSessions, _ := c.store.ListSessions(ctx, g.Resource, 0, true)
	for _, s := range openSessions {
		if s.LockID != "" {
			expectedLocks[s.LockID] = true
		}
	}
	if l, err := c.locker.GetLock(ctx, newRef); err == nil && expectedLocks[l.GetLockId()] {
		if err := c.locker.Unlock(ctx, newRef, l); err != nil {
			log.Warn().Err(err).Msg("could not release new-reference lock")
		}
	}

	// determine the predecessor before the transition (the marker is still
	// readable), so sessions can be rebound to it.
	prevSeq, _ := c.predecessorSeq(ctx, g)

	if _, err := c.store.TransitionGeneration(ctx, g.Resource, g.Sequence,
		[]GenState{GenFreezing, GenMoving}, GenAborted); err != nil {
		return err
	}

	if prevSeq > 0 {
		// rebind sessions that had been moved to the aborted generation.
		rebound, err := c.store.ListSessions(ctx, g.Resource, g.Sequence, true)
		if err != nil {
			return err
		}
		for _, s := range rebound {
			s.Generation = prevSeq
			if err := c.store.SaveSession(ctx, s); err != nil {
				return err
			}
		}
		if err := c.store.ClearSuperseded(ctx, g.Resource, prevSeq); err != nil {
			return err
		}
	}
	log.Warn().Err(reason).Msg("generation aborted; serving previous generation")
	return reason
}

// --- callbacks / close / unlock ---

// HandleCallback applies an app callback idempotently. Repeated callbacks are
// no-ops; old-generation saves after commit are rejected; close drives the
// cleanup of old references.
func (c *Coordinator) HandleCallback(ctx context.Context, cb *Callback) error {
	sess, err := c.store.GetSession(ctx, cb.SessionID)
	if err != nil {
		return err
	}
	if sess.Resource != cb.Resource {
		return wrap(ErrGenerationMismatch, "callback resource does not match session")
	}
	isNew, err := c.store.RecordCallback(ctx, cb.Resource, cb.SessionID, cb.Seq, cb.Kind)
	if err != nil {
		return err
	}
	if !isNew {
		// crash window between recording the callback and applying it: a
		// retried close must still take effect (idempotent).
		if cb.Kind == CallbackClosed && sess.IsOpen() {
			if err := c.store.CloseSession(ctx, sess.ID, c.clock.Now()); err != nil {
				return err
			}
			return c.cleanupOldGenerations(ctx, cb.Resource)
		}
		return nil
	}

	switch cb.Kind {
	case CallbackSaved:
		if cb.Generation != sess.Generation {
			return wrap(ErrGenerationMismatch, "save callback generation does not match session")
		}
		ok, err := c.generationCommittable(ctx, cb.Resource, cb.Generation)
		if err != nil {
			return err
		}
		if !ok {
			return wrap(ErrGenerationMismatch, "save callback at generation %d rejected", cb.Generation)
		}
	case CallbackClosed:
		if err := c.store.CloseSession(ctx, sess.ID, c.clock.Now()); err != nil {
			return err
		}
		return c.cleanupOldGenerations(ctx, cb.Resource)
	}
	return nil
}

// CloseSession closes a session and cleans up any old generations that no
// session is bound to.
func (c *Coordinator) CloseSession(ctx context.Context, sessionID string) error {
	sess, err := c.store.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if err := c.store.CloseSession(ctx, sessionID, c.clock.Now()); err != nil {
		return err
	}
	return c.cleanupOldGenerations(ctx, sess.Resource)
}

func (c *Coordinator) cleanupOldGenerations(ctx context.Context, r ResourceID) error {
	gens, err := c.store.ListGenerations(ctx, r)
	if err != nil {
		return err
	}
	// old references are cleaned only after the last session of the whole
	// resource closes: count open sessions across all generations.
	n, err := c.store.CountOpenSessions(ctx, r, 0)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	for _, g := range gens {
		if g.State.IsInProgress() {
			continue
		}
		if g.State != GenAborted && g.SupersededBy == 0 {
			continue // serving generation, even with no open sessions
		}
		if err := c.store.DeleteGeneration(ctx, r, g.Sequence); err != nil {
			return err
		}
		cleanLog := c.log(ctx, r)
		cleanLog.Info().Int64("generation", g.Sequence).Msg("cleaned old generation")
	}
	return nil
}

// Unlock is the generation-constrained unlock gate: old-reference unlocks are
// rejected; only the current generation's holder may unlock.
func (c *Coordinator) Unlock(ctx context.Context, r ResourceID, ref *provider.Reference, lock *provider.Lock) error {
	gens := mustList(ctx, c, r)
	if len(gens) == 0 {
		// resource never participated in a generation: plain unlock, no gate.
		return c.locker.Unlock(ctx, ref, lock)
	}

	g, err := c.store.GetInProgressGeneration(ctx, r)
	if err != nil {
		return err
	}
	if g != nil {
		// unlock is serialized with the move: it is rejected with a
		// retriable freeze during both freezing and moving, so it can
		// never delete a lock the generation protocol is taking over.
		return wrap(ErrFrozen, "cannot unlock generation %d while it is %s", g.Sequence, g.State)
	}
	if cur := servingGeneration(gens); cur != nil && cur.NewPath != "" {
		// an ID-only reference is resolved by the storage; an explicit stale
		// path is rejected.
		if ref.GetPath() != "" && ref.GetPath() != cur.NewPath {
			return wrap(ErrGenerationMismatch, "unlock reference %s is not the current path", ref.GetPath())
		}
	}
	if err := c.locker.Unlock(ctx, ref, lock); err != nil {
		return err
	}
	// releasing the write lock ends the editing sessions bound to it (the
	// spec allows an unlock event to drive session close). Idempotent.
	return c.closeSessionsForLock(ctx, r, lock.GetLockId())
}

// closeSessionsForLock closes open sessions correlated with lockID and runs
// the old-generation cleanup. Returns nil when no session matches.
func (c *Coordinator) closeSessionsForLock(ctx context.Context, r ResourceID, lockID string) error {
	if lockID == "" {
		return nil
	}
	sessions, err := c.store.ListSessions(ctx, r, 0, true)
	if err != nil {
		return err
	}
	closedAny := false
	for _, s := range sessions {
		if s.LockID == lockID {
			if err := c.store.CloseSession(ctx, s.ID, c.clock.Now()); err != nil {
				return err
			}
			closedAny = true
		}
	}
	if closedAny {
		return c.cleanupOldGenerations(ctx, r)
	}
	return nil
}

// IsTracked reports whether the coordinator holds any generation for the
// resource: gates skip resources that were never part of a generation move.
func (c *Coordinator) IsTracked(ctx context.Context, r ResourceID) (bool, error) {
	gens, err := c.store.ListGenerations(ctx, r)
	if err != nil {
		return false, err
	}
	return len(gens) > 0, nil
}

func mustList(ctx context.Context, c *Coordinator, r ResourceID) []*Generation {
	gens, err := c.store.ListGenerations(ctx, r)
	if err != nil {
		return nil
	}
	return gens
}

// Recover resumes all in-progress generations after a restart and finishes
// any abort whose rollback was interrupted by a crash.
func (c *Coordinator) Recover(ctx context.Context) error {
	gens, err := c.store.ListRecoverableGenerations(ctx)
	if err != nil {
		return err
	}
	for _, g := range gens {
		log := c.log(ctx, g.Resource)
		log.Info().Int64("generation", g.Sequence).Str("state", string(g.State)).
			Msg("recovering generation")
		if _, err := c.runOrJoin(ctx, g); err != nil {
			return err
		}
	}
	return c.finishAbortedGenerations(ctx)
}

// finishAbortedGenerations completes an interrupted abort: sessions still
// bound to an aborted generation are rebound to the predecessor (the
// greatest non-aborted generation below it) and the predecessor marker is
// cleared. Idempotent: once every session left the aborted generation the
// marker cleanup is the only remaining write.
func (c *Coordinator) finishAbortedGenerations(ctx context.Context) error {
	aborted, err := c.store.ListAbortedGenerations(ctx)
	if err != nil {
		return err
	}
	for _, g := range aborted {
		sessions, err := c.store.ListSessions(ctx, g.Resource, g.Sequence, true)
		if err != nil {
			return err
		}

		// predecessor is determined whether or not sessions remain: the
		// marker may be the only part of the rollback left to finish.
		var prev *Generation
		for _, x := range mustList(ctx, c, g.Resource) {
			if x.State != GenAborted && x.Sequence < g.Sequence {
				prev = x
			}
		}
		if prev == nil {
			// no predecessor to return the sessions to; leave for a later
			// recovery rather than inventing a generation.
			continue
		}
		for _, s := range sessions {
			s.Generation = prev.Sequence
			if err := c.store.SaveSession(ctx, s); err != nil {
				return err
			}
		}
		// finish the rollback only while the marker still points at this
		// aborted generation; a marker a later generation set is left
		// untouched. Idempotent: a second recovery finds marker==0.
		if err := c.store.ClearSupersededIf(ctx, g.Resource, prev.Sequence, g.Sequence); err != nil {
			return err
		}
	}
	return nil
}
