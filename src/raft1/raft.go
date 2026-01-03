package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// =================== Raft Node State Data Structures ==================

type LogEntry struct {
	Term int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	// Lock to protect shared access to this peer's state.
	mu sync.Mutex

	// Condition var guarded by 'mu' for
	// checking whether it's time to apply newly committed logs.
	applyCond sync.Cond

	// Channel on which the tester or service
	// expects Raft to send ApplyMsg messages
	applyCh chan raftapi.ApplyMsg

  // RPC end points of all peers.
	peers []*labrpc.ClientEnd

  // Object to hold this peer's persisted state.
	persister *tester.Persister

	// This peer's index into peers[].
	me int

	dead int32 // Set by Kill().

	// Current role.
	role	int // 0: Follower, 1: Candidate, 2: Leader

	// Latest term server has seen.
	Term int

	// CandidateId that received vote in the current term (or -1 if none).
	Vote	int // Index among peers array.

	// Election deadline: timestamp when we should start an election.
	// Routinely refreshed by heartbeats.
	electionDeadline time.Time

	// Array of log entries.
	// rf.log[0] will contain lastFrozenTerm/lastFrozenIndex from snapshot.
	// Physical entries start at rf.log[1:].
	// To acces logical index 'i' : 'rf.log[i - rf.lastFrozenIndex]'.
	log []LogEntry

	// Logical index of highest log entry known to be committed.
	commitIndex int

	// Logical index of highest log entry applied to state machine.
	lastApplied int

	// [Leader] For each peer server, index of next log entry to send.
	// 					Should be initialized upon winning election.
	nextIndex []int

	// [Leader] For each peer server,
	// 					index of highest log entry known to be replicated.
	// 					Should be initialized upon winning election.
	matchIndex []int

	// The logical index of the last entry included in the snapshot.
	// This entry is effectively stored at rf.log[0].
	lastFrozenIndex int

	// The term of the last entry included in the snapshot.
	lastFrozenTerm	int
}

// ============================== Init ==================================

// Make() creates a new raft peer that implements the raft interface.
// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. 'persister' is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. 'applyCh' is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.mu = sync.Mutex{}
	rf.applyCond = *sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	rf.Term = 0;
	rf.role = 0; // follower
	rf.Vote = -1;

	rf.log = make([]LogEntry, 1)
 	// Put value at index 0 so 1-indexing will work out of the box.
	rf.log[0] = LogEntry{Term: 0}

	// schedule next election nondeterministically
	rf.resetElectionDeadline()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start background goroutines
	go rf.electionTicker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}

// ======================== Persistent State ============================

// Encode current Raft state (metadata and log)
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)

  e.Encode(rf.Term)
  e.Encode(rf.Vote)
  e.Encode(rf.log)

  e.Encode(rf.lastFrozenIndex)
  e.Encode(rf.lastFrozenTerm)

  return w.Bytes()
}

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	raftstate := rf.encodeState()
	snapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, snapshot)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastFrozenIndex int
	var lastFrozenTerm int

	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	d.Decode(&lastFrozenIndex)
	d.Decode(&lastFrozenTerm)

	rf.Term = currentTerm
	rf.Vote = votedFor
	rf.log = log
	rf.lastFrozenIndex = lastFrozenIndex
	rf.lastFrozenTerm = lastFrozenTerm

	// If we crashed, our commitIndex must be at least as high as the snapshot.
	// We can't uncommit things that are in the snapshot.
	rf.commitIndex = lastFrozenIndex
	rf.lastApplied = lastFrozenIndex
}

/* ======================== Snapshots Utils =============================
 * When we dump the log to a snapshot, rf.log[i] will not be the ith
 * entry but the ith entry after the ones in the snapshot. We abstract this
 * away by providing helper functions. Caller must be holding the lock. */

// Return the logical index of the last log entry.
func (rf *Raft) getLastLogIndex() int {
	return rf.lastFrozenIndex + len(rf.log) - 1
}

// Returns the Term of the last log entry.
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

// Convert from logical index 'i' to physical index within 'rf.log'.
// Returns negative value if the index is not within the physical log.
func (rf *Raft) getPhysicalIndex(i int) int {
	return i - rf.lastFrozenIndex
}

// Given logical log entry index i, returns its term.
// Returns -1 if the index is not within the physical log.
func (rf *Raft) getTerm(i int) int {
	phys := rf.getPhysicalIndex(i)
	if phys < 0 || phys >= len(rf.log) {
		return -1
	} else {
		return rf.log[phys].Term
	}
}

/* =================== Raft API (raftapi/raftapi.go) ====================
 * These methods are exposed to the server/tester.
 * They will run concurrently, they must hold the lock. */

// Return current term and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Term := rf.Term
	isLeader := rf.role == 2
	return Term, isLeader
}

// How many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

// The service says it has created a snapshot that has
// all info up to and including index. This means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we already created a more recent snapshot,
	// or the commitIndex hasn't reached the requested point yet,
	// fail early.
	if index <= rf.lastFrozenIndex || index > rf.commitIndex {
		return
	}

	// Physical index of the snapshot point.
	phys := rf.getPhysicalIndex(index)

	// Create new log.
	newLog := make([]LogEntry, len(rf.log)-phys)
	copy(newLog, rf.log[phys:])

	// Update state.
	rf.lastFrozenTerm = newLog[0].Term
	rf.lastFrozenIndex = index
	rf.log = newLog

	// Persist both the state and the snapshot
	raftstate := rf.encodeState()
  rf.persister.Save(raftstate, snapshot)
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current term. The
// third return value is true if this server believes it is the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// If the Raft instance has been killed, we still return gracefully.
	isDead := rf.killed()
	if isDead {
		return -1, -1, false
	}

	rf.mu.Lock()
	isLeader := rf.role == 2

	// If this server isn't the leader, we return false immediately.
	if !isLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	// At this point we must be the leader,
	// so we start the agreement and return immediately.
	term := rf.Term
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.Term})
	index := rf.getLastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index+1

	rf.persist()
	rf.mu.Unlock()

	// Immediately send heartbeat.
	go rf.broadcastAppendEntries()

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ======================= RequestVote RPC ==============================

// RequestVote RPC arguments structure.
// Field names must start with capital letters.
type RequestVoteArgs struct {
	// Candidate's term
	Term int

	// Candidate requesting vote
	CandidateId int // index among peers array

	// Logical index of candidate's last log entry,
	// for voter to verify we're synced.
	LastLogIndex int

	// Term of candidate's last log entry, for voter to verify we're synced.
	LastLogTerm int
}

// RequestVote RPC reply structure.
// Field names must start with capital letters.
type RequestVoteReply struct {
	// Current term for candidate to update itself.
	Term int

	// True iff candidate received vote.
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If we see a term higher than ours, we update our term
	// and become a follower.
	if args.Term > rf.Term {
		rf.Term = args.Term
		rf.role = 0 // follower
		rf.Vote = -1
		rf.persist()
	}

	// Always return our current term to the candidate,
	// so they can update if they are behind.
	reply.Term = rf.Term

	// If we got a vote request with an older term,
	// we must reply with a failure and not grant the vote.
	if rf.Term > args.Term {
		reply.VoteGranted = false
		return
	}

	// We can't elect a ledear with corrupted old log entries as it
	// may overwrite commited logs onto followers. (Election Restriction,
	// Section 5.4.1 of the Raft paper)

	// We deny vote if:
	// - Our last term is greater than candidate's last term
	// - Or terms are equal, but our log is longer.
	myLastLgIdx := rf.getLastLogIndex()
	myLastLgTerm := rf.getLastLogTerm()
	logIsUpToDate := (args.LastLogTerm > myLastLgTerm ||
		(args.LastLogTerm == myLastLgTerm && args.LastLogIndex >= myLastLgIdx))
	if !logIsUpToDate {
		reply.VoteGranted = false
		return
	}

	// If we already voted for some other server during the current term,
	// we can't vote for this.
	if rf.Vote != -1 && rf.Vote != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// Finally if we are here we're happy to vote for this server.
	reply.VoteGranted = true
	rf.Vote = args.CandidateId
	rf.persist()
	rf.resetElectionDeadline() // We just voted, so we reset the election timer.
}

// Send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// Expects RPC arguments in args.
// Fills in *reply with RPC reply, so caller should pass &reply.
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs,
																reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ======================= AppendEntries RPC ============================

// As in Figure 2 of the paper.
type AppendEntriesArgs struct {
	Term 			int
	LeaderId 	int

	// Logical index of log entry immediately preceding new ones.
	PrevLogIndex 	int

 	// Term of PrevLogIndex entry
	PrevLogTerm 	int

 	// Log entries to store (empty for heartbeat)
	Entries 			[]LogEntry

	// Logical index of leader's commitIndex
	LeaderCommit 	int
}

// As in Figure 2 of the paper.
type AppendEntriesReply struct {
	Term 		int // Current term, for leader to update itself.
	Success bool

	/* Fast backup: retry from previous term instead of previous entry. */
	ConflictIndex int // Logical index of first entry in the conflicting term.
	ConflictTerm 	int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
															reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if request term is outdated.
	if args.Term < rf.Term {
		reply.Success = false
		reply.Term = rf.Term
		return
	}

	// At this point the term of the request must be up-to-date.

	// Reset next election target: we just got an RPC from a valid leader.
	rf.resetElectionDeadline()

	// Ensure to be follower and keep term updated
	if args.Term > rf.Term || rf.role != 0 {
		rf.Term = args.Term
		rf.role = 0 // follower
		rf.Vote = -1
		rf.persist()
	}

	// Reply false if prev log index is higher than our rf.log length,
	// so they can help us get back up from where we are.
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Success = false
		reply.Term = rf.Term
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1 // Code for no conflict term, just length
		return
	}

	physPrevLogIndex := rf.getPhysicalIndex(args.PrevLogIndex)

	// If leader is sending a PrevLogIndex that is compacted inside our snapshot,
  // we handle it as a conflict: we tell the leader to back up
	// to the end of our snapshot.
	if physPrevLogIndex < 0 {
		reply.Success = false
		reply.Term = rf.Term
		// Tell leader to try nextIndex starting just after our snapshot
		reply.ConflictIndex = rf.lastFrozenIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// Reply false if we have the entry but terms mismatch,
	// so they can help us refresh the entire term.
	if rf.log[physPrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.Term
		reply.ConflictTerm = rf.log[physPrevLogIndex].Term

		// Find the very first physical index of the ConflictTerm
		idx := physPrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == reply.ConflictTerm {
			idx--
		}

		// NOTE: ConflictIndex is a logical index.
		reply.ConflictIndex = rf.lastFrozenIndex + idx

		return
	}

	// At this point we are sure the given request has new valid entries for us
	// and the leader is up-to-date with our state.
	// We will proceed appending the new given log entrie from the leader.

	// If previous entry is inside the snapshot, we can't verify it.
	// But this is ok, it's like a conflict. Ask leader to sync us up.
	if physPrevLogIndex < 0 {
		reply.Success = false
		reply.Term = rf.Term
		reply.ConflictIndex = rf.lastFrozenIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// If existing entry conflicts with new one,
	// immediately communicate failure to the leader.
	if rf.log[physPrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.Term
		return
	}

	// Append any new entries not already in the log
	start := args.PrevLogIndex + 1
	for i, logEntry := range args.Entries {
		idx := start + i // logical

		physIdx := rf.getPhysicalIndex(idx)

		if physIdx < len(rf.log) {
			// If entry exists but terms conflict, delete this and everything after.
			if rf.log[physIdx].Term != logEntry.Term {
				rf.log = rf.log[:physIdx] // Truncate log
				rf.log = append(rf.log, logEntry) // Append new
			}
			// If terms match, do nothing.
		} else {
			// New entry, just append.
			rf.log = append(rf.log, logEntry)
		}
	}

	rf.persist()

	// If leaderCommit > commitIndex,
	// set commitIndex = min(leaderCommit, index of last new entry).
	if args.LeaderCommit > rf.commitIndex {
		// Update commit index
		indexOfLastNewEntry := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, indexOfLastNewEntry)

		// Wake up applier to apply newly committed entries
		rf.applyCond.Broadcast()
	}

	reply.Success = true

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
																	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ======================= InstallSnapshot RPC ==========================

type InstallSnapshotArgs struct {
    Term              int
    LeaderId          int

		// The logical index up to which the snapshot covers,
    LastIncludedIndex int
		// and its term.
    LastIncludedTerm  int

		// The snapshot.
    Data              []byte
}

type InstallSnapshotReply struct {
    Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,
																reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.Term

	if args.Term < rf.Term {
			rf.mu.Unlock()
			return
	}

	// Higher term, ensure we are followers.
	if args.Term > rf.Term {
			rf.Term = args.Term
			rf.role = 0
			rf.Vote = -1
			rf.persist()
	}
	rf.resetElectionDeadline()

	// If we already have this snapshot or a newer one, return early.
	if args.LastIncludedIndex <= rf.lastFrozenIndex {
		rf.mu.Unlock()
		return
	}

	// We are ahead of the snapshot, ignore it.
	if args.LastIncludedIndex <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	// At this point, the snapshot must be valid and fresh for us.

	// Sanity check: everything in the snapshot must have been already
	// applied to the state machine.
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	// Save the snapshot.
	// Determine if we can keep any of our existing log.
	// If the snapshot index is within our log, we keep the tail.
	// Otherwise, we discard the whole log.
	physIndex := rf.getPhysicalIndex(args.LastIncludedIndex)
	var newLog []LogEntry
	if physIndex > 0 && physIndex < len(rf.log) &&
		rf.log[physIndex].Term == args.LastIncludedTerm {
			// We have the entry matching the snapshot last index/term.
			// Keep the subsequent log entries.
			newLog = make([]LogEntry, len(rf.log)-physIndex)
			copy(newLog, rf.log[physIndex:])
	} else {
			// Discard entire log, reset to dummy entry
			newLog = make([]LogEntry, 1)
			newLog[0] = LogEntry{Term: args.LastIncludedTerm}
	}

	// Update state
	rf.lastFrozenIndex = args.LastIncludedIndex
	rf.lastFrozenTerm = args.LastIncludedTerm
	rf.log = newLog

	// Update commitIndex and lastApplied if we jumped forward
	if rf.commitIndex < rf.lastFrozenIndex {
			rf.commitIndex = rf.lastFrozenIndex
	}
	if rf.lastApplied < rf.lastFrozenIndex {
			rf.lastApplied = rf.lastFrozenIndex
	}

	// Persist both state and snapshot
	raftstate := rf.encodeState()
	snapshot := args.Data
  rf.persister.Save(raftstate, snapshot)

	// Send ApplyMsg to service so the state machine loads the snapshot
	msg := raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.applyCh <- msg
}

// ============ Leader to Followers Entries Broadcasting ================

/* Send AppendEntries to all peers. It will acquire the rf.mu lock, so
 * caller must ensure not to hold it when calling broadcastAppendEntries. */
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	// Snapthot current term.
	term := rf.Term
	me := rf.me
	rf.mu.Unlock()

	// Async send heartbeat with 'Term' to each peer.
	for i := range rf.peers {
		if i == me { continue }

		go func(server int) {
			rf.mu.Lock()

			// If the log entry the follower needs is already discarded,
			// send them the snapshot so they can sync.
			if rf.nextIndex[server] <= rf.lastFrozenIndex {
				args := &InstallSnapshotArgs{
					Term:              term,
          LeaderId:          me,
          LastIncludedIndex: rf.lastFrozenIndex,
          LastIncludedTerm:  rf.lastFrozenTerm,
          Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}
				if rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// If we're not leaders anymore, let's return early and do nothing.
					if rf.role != 2 || rf.Term != term { return }

					// If in the meanwhile we became outdated,
					// let's step down as followers.
					if reply.Term > rf.Term {
						rf.Term = reply.Term
						rf.role = 0
						rf.Vote = -1
						rf.persist()
						return
					}

					// Otherwise, we can update follower state.
					rf.matchIndex[server] = args.LastIncludedIndex
					rf.nextIndex[server] = args.LastIncludedIndex + 1
				}
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1

			physPrevLogIndex := rf.getPhysicalIndex(prevLogIndex)

			if physPrevLogIndex < 0 {
				// Something went wrong.
				rf.mu.Unlock()
				return
			}

			// Create a copy of the entries we think are missing
			// on this follower's log.
			entries := make([]LogEntry, len(rf.log) - 1 - physPrevLogIndex)
			copy(entries, rf.log[physPrevLogIndex+1:])

			args := &AppendEntriesArgs{
				Term: 				term,
				LeaderId: 		me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: 	rf.log[physPrevLogIndex].Term,
				Entries: 			entries,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// If term changed or we are no longer leader, stop.
				if rf.Term != args.Term || rf.role != 2 {
					return
				}

				// Higher term: step down as follower with updated term.
				if reply.Term > rf.Term {
					rf.Term = reply.Term
					rf.role = 0 // follower
					rf.Vote = -1 // reset vote
					rf.persist()
					return
				}

				if reply.Success {
					// Follower accepted the new entries,
					// let's update their matchIndex and nextIndex.
					newMatchIndex := args.PrevLogIndex + len(args.Entries)
					if newMatchIndex > rf.matchIndex[server] {
						rf.matchIndex[server] = newMatchIndex
						rf.nextIndex[server] = rf.matchIndex[server] + 1

						// Check if enough followers accepted the entries so that
						// we can advance commitIndex.
						rf.updateCommitIndex()
					}
				} else {
					// Follower inconsistency. Let's help them back-up.

					if reply.ConflictTerm == -1 {
						// Follower's log is shorter than PrevLogIndex.
						// Let's skip it altogether.
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						// Term mismatch. Let's skip the term.

						lastIndexOfTerm := -1
						for i = len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								lastIndexOfTerm = i
								break
							}
						}

						if lastIndexOfTerm != -1 {
							rf.nextIndex[server] = rf.lastFrozenIndex + lastIndexOfTerm + 1
						} else {
							// We don't have that term, perhaps we were offline during
							// that term and it wasn't committed anyway. Let's skip the
							// entire follower log history.
							rf.nextIndex[server] = reply.ConflictIndex
						}
					}
				}
			}
		}(i)
	}
}

// Attempt updating the commit index, based on whether the majority of
// followers accepted the new entries. Must be called with lock held.
func (rf *Raft) updateCommitIndex() {
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		// Leader can only commit log entries from current term.
		if rf.getTerm(N) != rf.Term {
			continue
		}

		// Count how many servers have replicated up to index N
		count := 1 // count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		// If majority, commit
		if count > len(rf.peers)/2 {
			rf.commitIndex = N

			// Wake up applier to apply newly committed entries
			rf.applyCond.Broadcast()

			break // We found the highest N, no need to check lower
		}
	}
}

// ========================== Election ==================================

/* Refreshes the target election time randomly between 300 and 600 ms
 * into the future. It is up to the caller to ensure to hold the
 * rf.mu lock. before resetting the timer. */
func (rf *Raft) resetElectionDeadline() {
	ms := 300 + (rand.Int63() % 300)
	rf.electionDeadline = time.Now().Add(time.Duration(ms) * time.Millisecond)
}

// Transition to candidate and attempt election.
// Caller must release lock before calling this.
func (rf *Raft) AttemptSelfElection() {
	rf.mu.Lock()

	// If we are already the leader or we heard an heartbeat
	// that refreshed the election timer already, return early.
	if rf.role == 2 || !time.Now().After(rf.electionDeadline) {
		rf.mu.Unlock()
		return
	}

	// reset election target
	rf.resetElectionDeadline()

	// advance term
	rf.Term++

	// Vote for self
	rf.role = 1 // candidate
	rf.Vote = rf.me

	me := rf.me
	term := rf.Term
	peers := rf.peers

	myLastLgIdx := rf.getLastLogIndex()
	myLastLogTerm := rf.getLastLogTerm()

	rf.mu.Unlock()

	// Start counter from the 1 self-vote.
	// Ensure counter is guarded by the 'rf.mu' lock.
	votesReceived := 1

	// Send RequestVote RPCs to all other servers
	for i, _ := range peers {
		if i == rf.me { continue }

		go func(server int) {
			args := &RequestVoteArgs{
				Term: term,
				CandidateId: me,
				LastLogIndex: myLastLgIdx,
				LastLogTerm: myLastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Ensure we're still candidates accepting votes,
				// and the vote is for the current term.
				if rf.Term != args.Term || rf.role != 1 { return }

				if reply.Term > rf.Term {
					// Found server with higher term.
					// Stepping down as followers.
					rf.role = 0 // follower
					rf.Term = reply.Term
					return
				}

				if reply.VoteGranted {
					votesReceived++

					// Check for Majority.
					// No need to upgrade to leader if we're already leader.
					if votesReceived > len(rf.peers)/2 && rf.role != 2 {
						// Becoming Leader. Init leader state.
						rf.role = 2

						// Init nextIndex for each peer, with initial value len(rf.log).
						rf.nextIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
						}

						// Init matchIndex for each peer, with initial value 0.
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.nextIndex { rf.matchIndex[i] = 0 }

						// Send first heartbeats immediately.
						// Following heartbeats will be sent automatically by the ticker.
						go rf.broadcastAppendEntries()
					}
				}
			}
		}(i)
	}
}

// ==================== Background Goroutines ==========================

// Ticker to routinely send heartbeats when we are leader.
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.role == 2
		rf.mu.Unlock()

		if isLeader {
			rf.broadcastAppendEntries()
		}

		// Wait 100ms before sending next heartbeat,
		// sending around 10 heartbeats per second.
		time.Sleep(100 * time.Millisecond)
	}
}

// Ticker to routinely check for leader heartbeats when we are non-leaders,
// and propose election of ourselves as leader when we stop getting heartbeats
// as well as restart election when stuck in minorities vote partitions.
func (rf *Raft) electionTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isFollowerOrCandidate := rf.role == 0 || rf.role == 1
		isElectionTime := time.Now().After(rf.electionDeadline)
		rf.mu.Unlock()

		if isFollowerOrCandidate && isElectionTime {
				rf.AttemptSelfElection()
		}

		// Wait random amount between 150 and 350ms before checkign again.
		ms := 150 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// Background goroutine to apply newly committed logs
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		// No new entries to commit, wait.
		// This will free the lock until a broadcast on applyCond will happen.
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		start := rf.lastApplied + 1
		end := rf.commitIndex

		// Create a copy of the entries to apply
		toApply := make([]LogEntry, 0)
		for i := start; i <= end; i++ {
			physIdx := rf.getPhysicalIndex(i)
			toApply = append(toApply, rf.log[physIdx])
		}

		// Advance lastApplied to commitIndex
		rf.lastApplied = rf.commitIndex

		// Release the lock before communicating with applyCh,
		// which will require the lock.
		rf.mu.Unlock()

		for i, entry := range toApply {
			msg := raftapi.ApplyMsg{
				CommandValid: 	true,
				Command: 				entry.Command,
				CommandIndex:  	start + i,
			}
			rf.applyCh <- msg
		}
	}
}
