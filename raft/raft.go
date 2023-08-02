// NOTE: the TestFigure8Unreliable2C fails because the faster log replication has not been implemented
package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"mit_distributed_systems/labgob"
	"mit_distributed_systems/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	votedFor    int
	numVotes    int
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	log         []LogEntry
	// Channels
	votedChan       chan bool
	heartBeatChan   chan bool
	stepDownChan    chan bool
	voteCounterChan chan bool
	applyChan       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// this function needs to be called while lock is being held
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		panic("Something went wrong while encoding")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// this function needs to be called while lock is being held
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		panic("Something went wrong while decoding")
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { // if candidate's term lower
		reply.VoteGranted = false // don't grant vote
		return
	}
	if args.Term > rf.currentTerm { // if the request vote has a higher term, become a follower
		rf.toFollower(args.Term)
	}
	reply.VoteGranted = false // initially don't give vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// Could possibly do this in one if statement, but it seems more readable this way
		if args.LastLogTerm > rf.getLastTerm() { // candidate's last log term is higher
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.sendToNonBlockChan(rf.votedChan, true)
			return
		}
		if args.LastLogTerm == rf.getLastTerm() { // if the logs end with the same term
			if args.LastLogIndex >= rf.getLastIndex() { // if candidate's log is longer
				reply.VoteGranted = true // give it the vote
				rf.votedFor = args.CandidateId
				rf.sendToNonBlockChan(rf.votedChan, true)
				return
			}
		}
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok { // if we didn't get a reply
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE { // if we're not the candidate
		return // skip the vote
	}
	if args.Term != rf.currentTerm { // if the term we sent is different from our term now
		return // skip the vote
	}
	if reply.Term > rf.currentTerm { // if the follower's term is higher than ours (there's another leader)
		rf.toFollower(reply.Term)
		rf.persist()
		return // skip the vote
	}

	if reply.VoteGranted {
		rf.sendToNonBlockChan(rf.voteCounterChan, true) // already buffered, so probably not necessary
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm { // 1. Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm { // (All servers) If term T > currentTerm, set currentTerm = T and convert to follower
		rf.toFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	rf.sendToNonBlockChan(rf.heartBeatChan, true) // we got an rpc message from the leader so send heartbeat message to self
	lastIndex := rf.getLastIndex()
	// if the index is higher than the index of our items, the leader's log is longer than ours
	// or the element at prevLogIndex has a different term than the leader, return false so we can try again in next appendEntry request with a lower index
	if args.PrevLogIndex > lastIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...) // append leader's entries to the log

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		lastIndex := rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
	}
	// update the logs
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { // for each of the uncommited logs that need to be commited
		rf.applyChan <- ApplyMsg{ // send msg through apply chan for the test
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied++ // increase last applied
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if rf.state != LEADER {
		return
	}
	if args.Term != rf.currentTerm {
		return
	}
	if reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.sendToNonBlockChan(rf.stepDownChan, true)
		return
	}

	if !reply.Success { // if it wasn't successful
		rf.nextIndex[server]-- // lower the index for next try (this line is probably problematic)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		updatedMatchIndex := args.PrevLogIndex + len(args.Entries)
		// check that we're not outdated
		if updatedMatchIndex > rf.matchIndex[server] { // if not outdated
			rf.matchIndex[server] = updatedMatchIndex // update the match index for this server
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1 // update nextIndex
	}

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N

	for N := rf.getLastIndex(); N >= rf.commitIndex; N-- {
		count := 1 // count myself
		if rf.log[N].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				// update the logs
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					rf.lastApplied++
				}
				break
			}
		}
	}

}

// the service using Raft (e.g. a k/v server) wants to start
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	isLeader := rf.state == LEADER
	index := -1
	term := -1
	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm

		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		})
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
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

// this function
func (rf *Raft) handleServer() {
	for !rf.killed() {
		rf.mu.Lock()
		serverState := rf.state
		rf.mu.Unlock()
		switch serverState {
		// Followers:
		// 		reset election timer on heartbeat
		//		reset election timer on vote
		// 		handle election timeouts
		case FOLLOWER:
			select {
			case <-rf.votedChan: // skip re-election if we voted
			case <-rf.heartBeatChan: // skip re-election timer if we got a heart-beat
			case <-time.After(rf.getElectionTimeout()):
				// convert from follower to candidate and start election
				rf.toCandidate(FOLLOWER)
			}
		// Candidates need to handle:
		case CANDIDATE:
			select {
			case <-rf.stepDownChan: // we are already a follower, so next select iteration it will go to the follower case
			case <-time.After(rf.getElectionTimeout()):
				rf.toCandidate(CANDIDATE)
			case <-rf.voteCounterChan:
				rf.numVotes++
				if rf.numVotes == len(rf.peers)/2+1 {
					rf.toLeader()
				}
			}

		// Leaders need to handle:
		case LEADER:
			select {
			case <-rf.stepDownChan: // we are already a follower, so next select iteration it won't send the heartbeat
			case <-time.After(120 * time.Millisecond): // this seems to be the magic number, any lower than this and servers sometimes fail to agree, probably because of overlapping
				rf.mu.Lock()
				rf.heartBeat()
				rf.mu.Unlock()
			}
		}
	}
}
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE { // if we're not the candidate (maybe somebody else won the election before us)
		return // return
	}
	rf.cleanUpChans()
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastLogIndex := rf.getLastIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex
	}
	rf.heartBeat()
}
func (rf *Raft) toCandidate(originalState int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != originalState {
		return
	}
	// clean-up all the channels
	rf.cleanUpChans()
	// change state
	rf.state = CANDIDATE
	// increase current term
	rf.currentTerm++
	// vote for self
	rf.votedFor = rf.me
	rf.numVotes = 1
	rf.persist()
	rf.startElection()
}

// always check that lock is being held before calling this function!
func (rf *Raft) toFollower(term int) {
	initialState := rf.state
	// need to change state before asking leader (if server is the leader) to step down
	// so that in the next switch iteration it will go right to the follower case
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	// now that our state is the follower, we can check the initial state before the change
	if initialState != FOLLOWER { // if we are the leader
		rf.sendToNonBlockChan(rf.stepDownChan, true) // step down
	}
}

// always check that lock is being held before calling this function!
func (rf *Raft) startElection() {
	if rf.state != CANDIDATE {
		return
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendRequestVote(server, &args, &RequestVoteReply{})
	}
}

// always check that lock is being held before calling this function!
func (rf *Raft) heartBeat() {
	if rf.state != LEADER {
		return
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		entries := rf.log[rf.nextIndex[server]:]
		prevLogIndex := rf.nextIndex[server] - 1
		prevLogTerm := rf.log[prevLogIndex].Term
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      make([]LogEntry, len(entries)),
			LeaderCommit: rf.commitIndex,
		}
		copy(args.Entries, entries)
		go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
	}

}

// always check that lock is being held before calling this function!
func (rf *Raft) getLastIndex() int {
	return len(rf.log) - 1
}

// always check that lock is being held before calling this function!
func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getLastIndex()].Term
}

// always check that lock is being held before calling this function!
func (rf *Raft) cleanUpChans() {
	rf.heartBeatChan = make(chan bool)
	rf.votedChan = make(chan bool)
	rf.stepDownChan = make(chan bool)
	rf.voteCounterChan = make(chan bool, len(rf.peers)-1)
}
func (rf *Raft) getElectionTimeout() time.Duration {
	time := time.Duration(350+rand.Intn(250)) * time.Millisecond
	return time
}

// this allows us to send a message to the channel without blocking
// it avoids a lot of stalling and slowing down of servers which would lead to
// timing problems
func (rf *Raft) sendToNonBlockChan(c chan bool, x bool) {
	select {
	case c <- x:
	default:
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.log = make([]LogEntry, 0)
	rf.currentTerm = 0
	rf.heartBeatChan = make(chan bool)
	rf.votedChan = make(chan bool)
	rf.stepDownChan = make(chan bool)
	rf.voteCounterChan = make(chan bool, len(rf.peers)-1)
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.log = append(rf.log, LogEntry{Term: 0})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.handleServer()
	return rf
}
