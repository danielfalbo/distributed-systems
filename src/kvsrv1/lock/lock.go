package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
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
	return lk
}

func (lk *Lock) Acquire() {
	// Keep trying to acquire the lock until we succeed.
	// Use lk.l as key, out client ID lk.id as value,
	// and lk.v as version. Recall that lk.v is our copy
	// of the most recent version where we observed the kv
	// to be free or owned by us.
	err := rpc.Err("")
	for err != rpc.OK {
		err = lk.ck.Put(lk.l, lk.id, lk.v)

		// If we get ErrMaybe then we may have acquired it and waiting for it to
		// become Free could deadlock. Therefore, we want to be sure: we want to
		// disambiguate any ErrMaybe into either OK or ErrVersion.
		if err == rpc.ErrMaybe {
			value, _, _ := lk.ck.Get(lk.l)

			if value == lk.id {
				// If we are holding the lock, then we can go ahead just as if the Put
				// succeeded, because it did.
				err = rpc.OK
			} else {
				// Otherwise, we can go ahead as if the Put failed because of outdated
				// version, because it did.
				err = rpc.ErrVersion
			}
		}

		// Inside the following block either the Put failed with ErrVersion or we
		// got ErrMaybe and we disambiguated it to actually be an ErrVersion.
		// Either way, the last free observed version must be obsolete. Let's ping
		// the kv until we successfully get a reply and observe it to be Free.
		if err == rpc.ErrVersion { // if err == rpc.ErrVersion || err == rpc.ErrMaybe
			value, version, _ := Busy, lk.v, rpc.Err("")
			for value != Free {
				value, version, _ = lk.ck.Get(lk.l)
			}

			// At this point it must be that value is Free.
			lk.v = version
		}
	}

	// At this point we must have succeeded at acquiring the lock
	// by proposing 'lk.v', so the new latest observed kv version
	// of the lock free or owned by us is 'lk.v + 1'.
	lk.v++
}

func (lk *Lock) Release() {
	// In releasing the lock, we can afford an ErrMaybe:
	// 	- if we get rpc.OK it obviously means we succeeded
	// 	- if we get rpc.ErrMaybe it means our RPC must have been executed
	// 		at most once, because we held the lock and now our version is
	// 		out of sync with the kv server
	err := rpc.Err("")
	for err != rpc.OK  && err != rpc.ErrMaybe {
		err = lk.ck.Put(lk.l, Free, lk.v)
	}
}
