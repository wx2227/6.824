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
const NULL int = -1

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
	Command 	interface{}
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
	votedFor	int
	logEntries	[]LogEntry

	// Volatile state on all servers
	commitIndex int					// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int					// index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders
	nextIndex	[]int				// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int				// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh 	chan ApplyMsg
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
	Index 			int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.BeFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	success := false

	if args.Term < rf.currentTerm {
		go func() {
			send(rf.appendEntry)
		}()
		return
	}

	prevLogIndexTerm := -1
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logEntries) {
		prevLogIndexTerm = rf.logEntries[args.PrevLogIndex].Term
	}
	if prevLogIndexTerm != args.PrevLogTerm {
		go func() {
			send(rf.appendEntry)
		}()
		return
	}

	index := args.PrevLogIndex
	for i:=0; i< len(args.Entries); i++ {
		index ++
		if index >= len(rf.logEntries) {
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
			break
		}
		if rf.logEntries[index].Term != args.Entries[i].Term {
			rf.logEntries = rf.logEntries[:index]
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
			break
		}
	}

	success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
		rf.UpdateLastApplied()
	}
	reply.Success = success
	go func() {
		send(rf.appendEntry)
	}()
	return
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
	Term 			int
	VoteGranted 	bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	if args.Term > term {
		rf.mu.Lock()
		rf.BeFollower(args.Term)
		rf.mu.Unlock()
		send(rf.leaderElected)
	}

	// If votedFor is null or candidateId, and candidate's log is at least up-to-date as receiver's log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	success := false
	if args.Term < rf.currentTerm {
	} else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {
	} else if args.LastLogTerm < rf.GetLastLogTerm() {

	} else if args.LastLogTerm == rf.GetLastLogTerm() && args.LastLogIndex < rf.GetLastLogIndex() {

	} else {
		rf.votedFor = args.CandidateId
		success = true
		// after vote granted, the node reset its election time out
		go func() {
			send(rf.leaderElected)
		}()
	}

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



func (rf *Raft) GetLastLogIndex() int{
	return len(rf.logEntries) - 1
}

func (rf *Raft) GetLastLogTerm() int{
	if len(rf.logEntries) == 0 {
		return -1
	}
	return rf.logEntries[len(rf.logEntries)-1].Term
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
	//////fmt.Print("%d become follower\n", rf.me)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = NULL
}

//
// Machine transfers from follower to candidate, and start election process
//
func (rf *Raft) BeCandidate() {
	//////fmt.Print("%d becomes candidate\n", rf.me)
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

//
// The machine transfers from candidate to leader and prevent new elections
//
func (rf *Raft) BeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.GetLastLogIndex() + 1
	}
}

func send(ch chan bool) {
	select {
	case <- ch:
	default:
	}
	ch <- true
}

func (rf *Raft) StartElection() {
	var count int32 = 1

	for i, _ := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				if rf.state != Candidate {
					rf.mu.Unlock()
					return
				}
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.GetLastLogIndex(),
					LastLogTerm:  rf.GetLastLogTerm(),
				}
				rf.mu.Unlock()
				reply := RequestVoteReply{}

				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// candidate discovers new term then transfers to follower
					// or leader discovers server with higher term and transfers to follower
					if reply.Term > rf.currentTerm {
						rf.BeFollower(reply.Term)
						//go func() {
						//	send(rf.leaderElected)
						//}()
						return
					}
					if reply.VoteGranted {
						atomic.AddInt32(&count, 1)
					}
					if atomic.LoadInt32(&count) > int32(len(rf.peers)/2) {
						rf.BeLeader()
						go func() {
							send(rf.leaderElected)
						}()
					}
				}
			}(i)
		}
	}

}

//
// update the commit index according to the rule
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
//
func (rf *Raft) UpdateCommitIndex() {
	////fmt.Print("try to update commit index for peer %d\n", rf.me)
	////fmt.Print("leader %d length of logs: %d\n", rf.me, len(rf.logEntries))
	////fmt.Print("leader %d commitIndex: %d\n", rf.me, rf.commitIndex)
	for N:= len(rf.logEntries)-1; N>rf.commitIndex; N-- {
		count := 1
		for i, index := range rf.matchIndex {
			if i == rf.me {
				continue
			}
			//fmt.Print("log length: %d, N: %d, matchindex: %d ",len(rf.logEntries) , N, index)
			//fmt.Print("match index %d for peer %d\n", index, j)
			if index >= N {
				count += 1
				//fmt.Print("count: %d\n", count)
			}
			//fmt.Print("term of log: %d, current term: %d\n", rf.logEntries[N].Term, rf.currentTerm)
			//fmt.Print("count: %d, majority: %d\n", count, len(rf.peers)/2)
			if count > len(rf.peers)/2 && rf.logEntries[N].Term == rf.currentTerm {
				//fmt.Print("log length: %d, N: %d, matchindex: %d peers: %d ",len(rf.logEntries) , N, index, len(rf.peers))
				//fmt.Print("count: %d\n", count)
				//fmt.Print("update the commit index from %d to %d for peer %d\n", rf.commitIndex, N, rf.me)
				rf.commitIndex = N
				rf.UpdateLastApplied()
				break
			}
		}
	}
}

//
// update lastApplied by rule
// if commitIndex > lastApplied, increment lastApplied
// apply log[lastApplied] to state machine
//
func (rf *Raft) UpdateLastApplied() {
	////fmt.Print("update the last applied index for peer %d\n", rf.me)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		log := rf.logEntries[rf.lastApplied]
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
	}
}

//
// The elected leader start to append log to other peers (start with heartbeats for now)
//
func (rf *Raft) StartAppendLog() {
	var count int32 = 1
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			for{
				rf.mu.Lock()
				//fmt.Printf("follower %d lastlogindex: %v, nextIndex: %v\n",i, rf.GetPrevLogIndex(i), rf.nextIndex[i])
				//fmt.Print("sending log entries from leader %d to peer %d for term %d\n", rf.me, i, rf.currentTerm)
				//fmt.Print("nextIndex:%d\n", rf.nextIndex[i])
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.GetPrevLogIndex(i),
					PrevLogTerm:  rf.GetPrevLogTerm(i),
					Entries:      append(make([]LogEntry, 0), rf.logEntries[rf.nextIndex[i]:]...),
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(i, &args, &reply)

				if !ok {
					return
				}
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.BeFollower(reply.Term)
					go func() {
						send(rf.appendEntry)
					}()
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
					//fmt.Print("leader: %v, for peer %v, match index: %d, next index: %d, peers: %d\n", rf.me, i, rf.matchIndex[i], rf.nextIndex[i], len(rf.peers))
					atomic.AddInt32(&count, 1)
					if atomic.LoadInt32(&count) > int32(len(rf.peers)/2) {
						//fmt.Print("leader %d reach agreement\n, args.prevlogindex:%d, len:%d\n", rf.me, args.PrevLogIndex, len(args.Entries))
						rf.UpdateCommitIndex()
					}
					rf.mu.Unlock()
					return
				} else {
					//fmt.Printf("peer %d reset the next index from %d to %d\n", i, rf.nextIndex[i], rf.nextIndex[i]-1)
					if rf.nextIndex[i] > 0 {
						rf.nextIndex[i]--
						rf.mu.Unlock()
					} else {
						rf.mu.Unlock()
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
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

	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.logEntries = make([]LogEntry, 1)

	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.leaderElected = make(chan bool)
	rf.appendEntry = make(chan bool)
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
					rf.mu.Lock()
					rf.BeCandidate()
					rf.mu.Unlock()
					go func() {
						rf.StartElection()
					}()
				case <- rf.leaderElected:
				case <- rf.appendEntry:

				}
			case Leader:
				rf.StartAppendLog()
				time.Sleep(heartBeat)
			}
		}
	}()

	return rf
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == Leader
	//If command received from client: append entry to local log, respond after entry applied to state machine (§5.3)
	if isLeader {
		index = rf.GetLastLogIndex() + 1
		newLog := LogEntry{
			rf.currentTerm,
			command,
		}
		rf.logEntries = append(rf.logEntries,newLog)
	}
	return index, term, isLeader
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
