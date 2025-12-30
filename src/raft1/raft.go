package raft

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// =================== Raft Node State Data Structure ===================

// A Go object implementing a single Raft peer.
type Raft struct {
	// Lock to protect shared access to this peer's state.
	mu sync.Mutex

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
	term int

	// CandidateId that received vote in the current term (or -1 if none).
	vote	int // Index among peers array.

	// Election deadline: timestamp when we should start an election.
	// Routinely refreshed by heartbeats.
	electionDeadline time.Time
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

	rf.term = 0;
	rf.role = 0; // follower
	rf.vote = -1;

	// schedule next election nondeterministically
	rf.resetElectionDeadline()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start tickers goroutines
	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}

// ======================== Persistent State ============================

// Save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
// See paper's Figure 2 for a description of what should be persistent.
//
// NOTE: Before you've implemented snapshots, you should pass nil as the
// 		second argument to persister.Save(). After you've implemented snapshots,
// 		pass the current snapshot (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

/* =================== Raft API (raftapi/raftapi.go) ====================
 * These methods are exposed to the server/tester.
 * They will run concurrently, they must hold the lock. */

// Return current term and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.term
	isLeader := rf.role == 2
	return term, isLeader
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
	// Your code here (3D).
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

	// As usual, if we see a term higher than ours, we update our term
	// and become a follower.
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = 0 // follower
		rf.vote = -1
	}

	// Always return our current term to the candidate,
	// candidate so they can update if they are behind.
	reply.Term = rf.term

	// If we got a vote request with an older term,
	// we must reply with a failure and not grant the vote.
	if rf.term > args.Term {
		reply.VoteGranted = false
		return
	}

	// If we already voted for some other server during the current term,
	// we can't vote for this. Otherwise, we're happy to vote for this.
	if rf.vote != -1 && rf.vote != args.CandidateId {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true

		rf.vote = args.CandidateId
		rf.resetElectionDeadline() // We just voted, so we reset the election timer.
	}
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

type AppendEntriesArgs struct {
	Term 			int
	LeaderId 	int
}

type AppendEntriesReply struct {
	Term 		int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
															reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if request term is outdated
	if args.Term < rf.term {
		reply.Success = false
		reply.Term = rf.term
	} else {
		reply.Success = true

		// Reset next election target: we just heard from a valid leader.
		rf.resetElectionDeadline()

		// Ensure to be follower and keep term updated
		rf.term = args.Term
		rf.role = 0 // follower
		rf.vote = -1
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
																	reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// ======================= Heartbeats ===================================

/* Send AppendEntries to all peers. It will acquire the rf.mu lock, so
 * caller must ensure not to hold it when calling sendHeartbeats. */
func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	// Snapthot current term.
	term := rf.term
	me := rf.me
	rf.mu.Unlock()

	// Async send heartbeat with 'term' to each peer.
	for i := range rf.peers {
		if i == me { continue }

		go func(server int) {
			args := &AppendEntriesArgs{
				Term: term,
				LeaderId: me,
			}
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				rf.mu.Lock()
				if reply.Term > rf.term {
					// Higher term: step down as follower with updated term.
					rf.term = reply.Term
					rf.role = 0 // follower
					rf.vote = -1 // reset vote
				}
				rf.mu.Unlock()
			}
		}(i)
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
	rf.term++

	// vote for self
	rf.role = 1 // candidate
	rf.vote = rf.me

	me := rf.me
	term := rf.term
	peers := rf.peers

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
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Ensure we're still candidates accepting votes,
				// and the vote is for the current term.
				if rf.term != args.Term || rf.role != 1 { return }

				if reply.Term > rf.term {
					// Found server with higher term.
					// Stepping down as followers.
					rf.role = 0 // follower
					rf.term = reply.Term
					return
				}

				if reply.VoteGranted {
					votesReceived++

					// Check for Majority.
					// No need to upgrade to leader if we're already leader.
					if votesReceived > len(rf.peers)/2 && rf.role != 2 {
						// Become Leader
						rf.role = 2 // leader

						// Send first heartbeats immediately.
						// Following heartbeats will be sent automatically by the ticker.
						go rf.sendHeartbeats()
					}
				}
			}
		}(i)
	}
}

// ============================ Tickers =================================

// Ticker to routinely send heartbeats when we are leader.
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.role == 2
		rf.mu.Unlock()

		if isLeader {
			rf.sendHeartbeats()
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
