package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
)

// The lock state will be:
// 		- empty string '' when the lock is free
// 		- client id otherwise.

const (
	// Empty string for when the lock state when free.
	Free = ""

	// Dummy busy value for when a non-Free value is needed.
	Busy = "_busy"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck 	kvtest.IKVClerk

	// Client ID.
	id	string

	// The string 'l' is the key we use to store the "lock state".
	l		string

	// We'll maintain the most recent version where we observe the lock to be
	// free or owned by us.
	v 	rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
// Use l as the key to store the "lock state".
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// Init lock with given ck, given l, new client ID, and latest free version 0.
	lk := &Lock{ck: ck, l: l, id: kvtest.RandValue(8), v: rpc.Tversion(0)}

	// Attempt lock inizialization in case we're the first client to ever connect.
	// It's ok for this to fail: it just means we're not the first client.
	lk.ck.Put(lk.l, Free, lk.v)

	return lk
}

func (lk *Lock) Acquire() {
	// Keep trying to acquire the lock until we suceed.
	// Use lk.l as key, out client ID lk.id as value,
	// and lk.v as version. Recall that lk.v is our copy
	// of the most recent version where we observed the kv
	// to be free or owned by us.
	for lk.ck.Put(lk.l, lk.id, lk.v) != rpc.OK {
		// If we are here, the Put failed,
		// the last free version observed must be obsolete.
		// Let's ping the kv until the version changes.
		// Once it's free, we can update 'lk.v'.
		value, version, err := Busy, lk.v, rpc.Err(rpc.ErrMaybe)
		for err != rpc.OK || value != Free {
			value, version, err = lk.ck.Get(lk.l)
			time.Sleep(1 * time.Second)
		}

		// At this point it must be that err is OK and value is Free.
		lk.v = version
	}

	// At this point we must have succeeded at acquiring the lock
	// by proposing 'lk.v', so the new latest observed kv version
	// of the lock free or owned by us is 'lk.v + 1'.
	lk.v++
}

func (lk *Lock) Release() {
	lk.ck.Put(lk.l, Free, lk.v)
}
