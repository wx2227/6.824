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
	//"fmt"
	"sync"
	"sync/atomic"
	"time"
)
import "labrpc"
import "math/rand"

// import "bytes"
// import "labgob"

var leaderElected chan bool // The channel to notify that leader is selected
var beFollower chan int // The channel to notify

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term 		int
	Index 		int

}

type State int
const (
	Follower State = 0
	Candidate State = 1
	Leader State = 2
)

type App struct {
	mutex			sync.Mutex
	raft 			*Raft
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state 		State				// State of the machine

	currentTerm int
	votedFor	*int
	logEntries	[]LogEntry

	// Volatile state on all servers
	commitIndex int					// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int					// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	nextIndex	[]int				// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int				// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	
	leaderElected chan bool
	appendEntry chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
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
}

type AppendEntriesArgs struct {
	Term 			int			// leader’s term
	LeaderId 		int			// so follower can redirect clients
	PrevLogIndex 	int			// index of log entry immediately preceding new ones
	PrevLogTerm 	int			// term of prevLogIndex entry
	Entries 		[]LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int			// leader’s commitIndex
}

type AppendEntriesReply struct {
	Term 			int
	Success			bool
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// rules for servers
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.BeFollower(args.Term)
		go func() {
			rf.appendEntry <- true
		}()
		return
	}
	success := false
	if args.Term < rf.currentTerm {

	}
	//else if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
	//
	//}
	reply.Term = rf.currentTerm
	reply.Success = success
	go func() {
		rf.appendEntry <- true
	}()



	//// if the term of leader is smaller than the follower's term
	//if args.Term < rf.currentTerm {
	//	reply.Term = rf.currentTerm
	//	reply.success = false
	//}
	//
	//// if there is no entry in the logs of the follower that
	//// can be matched with the previous log of the leader
	//available := false
	//
	//for _, existEntry := range rf.logEntries {
	//	if existEntry.term == args.PrevLogTerm && existEntry.index == args.PrevLogIndex {
	//		available = true
	//	}
	//}
	//
	//if available == false {
	//	reply.Term = rf.currentTerm
	//	reply.success = false
	//}
	//
	//// If an existing entry conflicts with a new one (same index but different terms)
	//// delete the existing entry and all that follow it
	//removeList := make([]int, 0)
	//for _, existEntry := range rf.logEntries {
	//	for _, entry := range args.Entries {
	//		if existEntry.index == entry.index && existEntry.term != entry.term {
	//			rf.logEntries = rf.logEntries[:existEntry.index]
	//			break
	//		}
	//		if existEntry.index == entry.index && existEntry.term == entry.term {
	//			removeList = append(removeList, entry.index)
	//		}
	//	}
	//}
	//
	//// Append any new entries not already in the log
	//for _, entry := range args.Entries {
	//	if len(removeList) >= 0 && entry.Index != removeList[0] {
	//		rf.logEntries = append(rf.logEntries, entry)
	//	} else if entry.Index == removeList[0]{
	//		removeList = removeList[1:]
	//	}
	//}
	//
	//rf.mu.Lock()
	//// update the commit index of the follower
	//if args.LeaderCommit > rf.commitIndex {
	//	rf.commitIndex = min(args.LeaderCommit, rf.logEntries[len(rf.logEntries)-1].index)
	//}
	//rf.mu.Lock()

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int
	CandidateId 	int
	LastLogIndex 	int
	LastLogTerm 	int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int
	VoteGranted 	bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	if args.Term > term {
		rf.mu.Lock()
		rf.BeFollower(args.Term)
		rf.mu.Unlock()
		rf.leaderElected <- true
	}

	////fmt.Printf("gathering vote from %d\n", rf.me)
	// term of current server

	// If votedFor is null or candidateId, and candidate's log is at least up-to-date as receiver's log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	success := false
	if args.Term < rf.currentTerm {
	} else if rf.votedFor != nil && *rf.votedFor != args.CandidateId {
	} else if args.LastLogTerm < rf.GetLastLogTerm() {

	} else if args.LastLogTerm == rf.GetLastLogTerm() && args.LastLogIndex < rf.GetLastLogIndex() {

	} else {
		rf.votedFor = &args.CandidateId
		success = true
		// after vote granted, the node reset its election time out
		go func() {
			rf.leaderElected <- true
		}()
	}

	////fmt.Printf("from %d gather %t\n", rf.me, success)
	reply.VoteGranted = success

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

func (rf *Raft) GetLastLogIndex() int{
	return len(rf.logEntries)-1
}

func (rf *Raft) GetLastLogTerm() int{
	if len(rf.logEntries) == 0 {
		return -1
	}
	return rf.logEntries[rf.GetLastLogIndex()].Term
}

func (rf *Raft) GetPrevLogIndex(i int) int {
	return rf.nextIndex[i]-1
}

func (rf *Raft) GetPrevLogTerm(i int) int {
	idx := rf.GetPrevLogIndex(i)
	if idx < 0 {
		return -1
	}
	return rf.logEntries[idx].Term
}

//
// Machine transfers from leader to follower
//
func (rf *Raft) BeFollower(term int) {
	////fmt.Printf("%d become follower\n", rf.me)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = nil
}

//
// Machine transfers from follower to candidate, and start election process
//
func (rf *Raft) BeCandidate() {
	////fmt.Printf("%d becomes candidate\n", rf.me)
	rf.state = Candidate
	rf.currentTerm += 1
}

//
// The machine transfers from candidate to leader and prevent new elections
//
func (rf *Raft) BeLeader() {
	////fmt.Printf("%d become leader\n", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.GetLastLogIndex() + 1
	}
}

func (rf *Raft) StartElection() {
	////fmt.Printf("Leader election begins\n")
	//rf.votedFor = &rf.me
	var count int32 = 1

	for i, _ := range rf.peers {
		if i != rf.me {
			////fmt.Printf("gather vote from peer %d\n ", i)
			go func(i int) {
				rf.mu.Lock()
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.GetLastLogIndex(),
					LastLogTerm:  rf.GetLastLogTerm(),
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}

				////fmt.Printf("send request vote to %d\n ", i)
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// candidate discovers new term then transfers to follower
					// or leader discovers server with higher term and transfers to follower
					if reply.Term > rf.currentTerm {

						//fmt.Printf("%d become follower during leader election\n", rf.me)
						rf.BeFollower(reply.Term)
						go func() {
							rf.leaderElected <- true
						}()
						return
					}
					if reply.VoteGranted {
						atomic.AddInt32(&count, 1)
					}
					if atomic.LoadInt32(&count) > int32(len(rf.peers)/2) {
						rf.BeLeader()
						go func() {
							rf.leaderElected <- true
						}()
					}
				}

			}(i)
		}
	}

}

//
// The elected leader start to append log to other peers (start with heartbeats for now)
//
func (rf *Raft) StartAppendLog() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.GetPrevLogIndex(i),
					PrevLogTerm:  rf.GetPrevLogTerm(i),
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				//fmt.Printf("heartbeat to %d\n", i)
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(i, &args, &reply)

				if !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Leader {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.BeFollower(reply.Term)
					return
				}
			}(i)
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = nil
	rf.logEntries = make([]LogEntry, 0)

	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	rf.leaderElected = make(chan bool)
	heartBeat := time.Duration(100)*time.Millisecond

	// You'll need to write code that takes actions periodically or after delays in time.
	// The easiest way to do this is to create a goroutine with a loop that calls time.Sleep().
	// Don't use Go's time.Timer or time.Ticker, which are difficult to use correctly.
	go func() {
		for {
			delay := time.Duration(rand.Intn(300) + 300)*time.Millisecond
			timer := time.NewTimer(delay)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			switch state {
			case Follower, Candidate:
				select {
				case <- timer.C:
					//fmt.Printf("Time expired, %d become candidate\n", rf.me)
					rf.mu.Lock()
					rf.BeCandidate()
					rf.mu.Unlock()
					rf.StartElection()
				case <- rf.leaderElected:
					//fmt.Printf("%d grant vote\n", rf.me)
				case <- rf.appendEntry:
					//fmt.Printf("Heart beat received\n")
				}
			case Leader:
				//fmt.Printf("heart beat\n")
				rf.StartAppendLog()
				time.Sleep(heartBeat)
			}
		}
	}()

	return rf
}

func (a *App) RPC(args interface{}, reply interface{}) {
	// ...
	//a.mutex.Lock()
	//i, _, _ := a.raft.Start(args)
	//// update some data structure so that apply knows to poke us later
	//a.mutex.Unlock()
	//// wait for apply to poke us
	//return
}

func (a *App) apply(index int, cmd interface{}) {
	//a.mutex.Lock()
	//switch cmd := cmd.(type) {
	//case GetArgs:
	//	// do the get
	//	// see who was listening for this index
	//	// poke them all with the result of the operation
	//	// ...
	//}
	//a.mutex.Unlock()
}
