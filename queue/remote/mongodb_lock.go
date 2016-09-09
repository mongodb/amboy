package remote

import (
	"os"
	"sync"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/mgo.v2"

	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/tychoish/grip"
)

const (
	lockTimeout        = time.Minute * 5
	retryInterval      = time.Second * 2
	lockUpdateInterval = time.Minute * 1
	operationTimeout   = time.Millisecond * 100
)

var machineName string

func init() {
	var err error
	machineName, err = os.Hostname()
	if err != nil {
		machineName = uuid.NewV4().String()
	}
}

// MongoDBJobLock provides an implementation of the JobLock interface
// that stores lock information and state in MongoDB.
type MongoDBJobLock struct {
	name       string
	last       *jobLock
	collection *mgo.Collection
	mutex      sync.RWMutex
	count      int
}

// NewMongoDBJobLock creates and returns a new lock instance. The
// operation also persists this lock into the collection and returns
// an error if there are problems with the collection.
func NewMongoDBJobLock(ctx context.Context, name string, collection *mgo.Collection) (*MongoDBJobLock, error) {
	l := &MongoDBJobLock{
		name:       name,
		collection: collection,
	}

	err := collection.FindId(name).One(l.last)
	if err != nil {
		l.last = &jobLock{
			Name:  name,
			Host:  machineName,
			Time:  time.Now(),
			Count: 1,
		}

		if ctx.Err() != nil {
			return nil, errors.New("operation canceled while re/loading lock")
		}

		info, err := collection.UpsertId(name, l.last)
		grip.Debugf("upserting lock %s [%+v] (err=%v): %+v",
			name, l.last, err, info)
		if err != nil {
			return nil, errors.Wrapf(err,
				"problem upserting lock for job=%s", name)
		}
	}

	l.count = l.last.Count

	return l, nil
}

type jobLock struct {
	Name   string    `bson:"_id" json:"_id" yaml:"_id"`
	Locked bool      `bson:"locked" json:"locked" yaml:"locked"`
	Time   time.Time `bson:"time" json:"time" yaml:"time"`
	Host   string    `bson:"host" json:"host" yaml:"host"`
	Count  int       `bson:"count" json:"count" yaml:"count"`
}

// Name returns the name of the job.
func (l *MongoDBJobLock) Name() string {
	return l.name
}

// IsLocked returns true if the Lock is currently held and false
// otherwise. IsLocked attempts to refresh the local state of the
// lock. If the operation encounters errors or takes longer than the
// timeout (100 millisecond), this method returns true.
func (l *MongoDBJobLock) IsLocked(ctx context.Context) bool {
	// because we have the lock in reload
	l.mutex.Lock()
	defer l.mutex.Unlock()

	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	return l.isLockedUnsafe(ctx)
}

// IsLockedElsewhere checks to see if another process or host owns the
// lock. Returns false if the lock is not held, or is held by this
// process.
func (l *MongoDBJobLock) IsLockedElsewhere(ctx context.Context) bool {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()

	if !l.isLockedUnsafe(ctx) {
		return false
	}

	if l.last.Host != machineName {
		return true
	}

	return false
}

// Lock blocks until it can get a lock. Additionally launches a
// background thread to update the timer, so that we can continue to
// hold the lock, which will expire after several minutes (defined in
// a constant) if the lock holder does not continue to update the log.
func (l *MongoDBJobLock) Lock(ctx context.Context) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	timer := time.NewTimer(0)
	defer timer.Stop()

getLockLoop:
	for {
		select {
		case <-ctx.Done():
			break getLockLoop
		case <-timer.C:
			err := l.reload()
			if err != nil {
				grip.Debug(err)
				timer.Reset(retryInterval)
				continue getLockLoop
			}

			err = l.releaseIfExpired(retryInterval / 2)
			if err != nil {
				grip.Debug(err)
				timer.Reset(retryInterval)
				continue getLockLoop
			}

			if l.last.Locked {
				timer.Reset(retryInterval)
				continue getLockLoop
			}

			// if we get here its not locked by anyone else, or
			// ourselves, so we can take the lock.
			err = l.update(&jobLock{
				Name:   l.name,
				Locked: true,
				Host:   machineName,
				Time:   time.Now(),
			})

			if err != nil {
				grip.Error(err)
				timer.Reset(retryInterval)
				continue getLockLoop
			}

			go l.updateLockClock(ctx)

			break getLockLoop
		}
	}
}

// Unlock releases the lock, checking first that the current process
// has right to hold the lock..
func (l *MongoDBJobLock) Unlock(ctx context.Context) {
	// the challenge here is not in actually unlocking it, but in
	// determining if we have the authority to release the lock.

	l.mutex.Lock()
	defer l.mutex.Unlock()

	timer := time.NewTimer(0)

unlockingLoop:
	for {
		select {
		case <-ctx.Done():
			break unlockingLoop
		case <-timer.C:
			err := l.reload()
			if err != nil {
				grip.Error(err)
				timer.Reset(retryInterval)
				continue unlockingLoop
			}

			// if it's not locked by this process, then we
			// shouldn't block this process.
			if l.last.Host != machineName {
				break unlockingLoop
			}

			// if we don't think this lock is being held then it's
			// safe to let go here.
			if !l.last.Locked {
				break unlockingLoop
			}

			// if we get here, then we can let go of the lock
			l.last.Locked = false
			err = l.update(l.last)
			if err != nil {
				grip.Error(err)
				timer.Reset(retryInterval)
				continue unlockingLoop
			}

			break unlockingLoop
		}
	}

}

//////////////////////////////
//
// Function to run in a background process
//
//////////////////////////////

// updateLockClock runs as a background process that lets go of locks
// that have timed out, and updates the timer periodically to prevent
// a lock from becoming too stale. By default these remote locks time
// out if the holding process doesn't update its "lease".

func (l *MongoDBJobLock) updateLockClock(ctx context.Context) {
	// all of the methods called in this function (which runs in a
	// go routine) take care of their own locking. This function
	// should not lock directly.
	timer := time.NewTimer(0)
	defer timer.Stop()

updateLoop:
	for {
		select {
		case <-ctx.Done():
			break updateLoop
		case <-timer.C:
			l.mutex.Lock()
			err := l.reload()
			l.mutex.Unlock()

			if err != nil {
				grip.Debug(err)
				timer.Reset(retryInterval)
				continue updateLoop
			}

			// if the lock is older than our timeout, then we
			// should yield the lock.
			l.mutex.Lock()
			err = l.releaseIfExpired(lockTimeout)
			l.mutex.Unlock()
			if err != nil {
				grip.Debug(err)
				timer.Reset(retryInterval)
				continue updateLoop
			}

			// if we don't think this lock is locked (by us,) then
			// this thread can exit.
			ctxForCheck, _ := context.WithTimeout(ctx, operationTimeout)

			if !l.isLockedUnsafe(ctxForCheck) {
				break updateLoop
			}

			// if the lock isn't held by this process then we
			// don't need to keep pinging the lock.
			if !l.isLocalOwner() {
				break updateLoop
			}

			// since we update a counter each time the lock
			// changes hands, if the lock has changed hands (and
			// changed back) since this thread started, then we
			// really want to stop pinging the lock.
			if l.hasCountMismatch() {
				break updateLoop
			}

			// if we get here; then we should update the lock.
			l.mutex.Lock()
			err = l.update(&jobLock{
				Name:   l.name,
				Locked: true,
				Host:   machineName,
				Time:   time.Now(),
			})
			l.mutex.Unlock()
			if err != nil {
				grip.Error(errors.Wrapf(err,
					"problem during routine lock bump for %s", l.name))
				timer.Reset(retryInterval)
				continue updateLoop
			}

			grip.Debugf("bumped lock time on %s", l.name)
			timer.Reset(lockUpdateInterval)
		}
	}
}

//////////////////////////////
//
// safe internal methods
//
//////////////////////////////

func (l *MongoDBJobLock) isLocalOwner() bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.last.Host == machineName
}

func (l *MongoDBJobLock) hasCountMismatch() bool {
	// we bump a counter on the lock each time we update it,
	// therefore, if the counter on the lock and our local lock
	// tracker are not the same, then someone has updated the lock
	// in the time since we last updated, then we don't actually have the lock.

	l.mutex.RLock()
	defer l.mutex.RUnlock()

	return l.count != l.last.Count
}

//////////////////////////////
//
// Unsafe internal methods that should be called within a lock.
//
//////////////////////////////

func (l *MongoDBJobLock) reload() error {
	return errors.Wrapf(l.collection.FindId(l.name).One(l.last),
		"problem reloading lock for %s", l.name)
}

func (l *MongoDBJobLock) update(new *jobLock) error {
	new.Count = l.last.Count + 1
	l.count = new.Count

	err := l.collection.UpdateId(l.name, new)
	grip.DebugWhenf(err == nil, "updated lock for %s", l.name)

	return errors.Wrapf(err, "problem updating lock for '%s'", l.name)
}

func (l *MongoDBJobLock) releaseIfExpired(timeout time.Duration) error {
	if time.Since(l.last.Time) > timeout {
		return errors.Wrapf(l.update(&jobLock{Name: l.name}),
			"problem updating lock '%s' to release after timeout %s",
			l.name, timeout.String())
	}

	return errors.Wrapf(l.reload(),
		"problem reloading %s after releasing a lock after timeout", l.name)
}

func (l *MongoDBJobLock) isLockedUnsafe(ctx context.Context) bool {
	// retry for endlessly in the error case, just assume
	// things are locked elsewhere to prevent danger.
	for {
		select {
		case <-ctx.Done():
			return true
		default:
			err := l.reload()
			grip.CatchDebug(err)
			if err == nil {
				return l.last.Locked
			}
		}
	}
}
