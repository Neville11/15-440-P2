//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = true

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

var RANGE, LOWER = 800, 700 //used for election Intervals

type RoleType int

const (
	Leader RoleType = iota
	Candidate
	Follower
)

//
// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
//
type ApplyCommand struct {
	Index   int
	Command interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux         sync.Mutex       // Lock to protect shared access to this peer's state
	peers       []*rpc.ClientEnd // RPC end points of all peers
	me          int              // this peer's index into peers[]
	role        RoleType
	currTerm    int
	votedFor    int
	numVotes    int          //votes acquired
	numSuceeded int          //successful appends
	logger      *log.Logger  // We provide you with a separate logger per peer.
	elecTimer   *time.Ticker //ticker for election Timeouts
	cmdCh       chan interface{}
	applyCh     chan ApplyCommand
	commitIdx   int   //idx of highest committed entry
	lastApplied int   //idx of highest entry applied to state machine
	nextIdx     []int //idx of next log entry to send per server
	matchIdx    []int //idx of highest entry known to be replicated per server
	log         []logEntry
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock() //CS for accessing state variables
	defer rf.mux.Unlock()

	me, term := rf.me, rf.currTerm
	isleader := rf.role == Leader

	rf.logger.Printf("Showing current state of server. Role is %v \n", rf.role)
	return me, term, isleader
}

type logEntry struct {
	Cmd  interface{}
	Term int //term when entry was received by leader
}

// RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	IsHeartbeat     bool //distinguish from actual Append requests
	PrevLogIdx      int  //idx of log entry preceeding new ones
	PrevLogTerm     int  //term of entry immediately preeceeding new ones
	LeaderCommitIdx int
	Entries         []logEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mux.Lock() //CS accessing raft DS variables
	// rf.logger.Printf("aquired lock for RPC response")

	reply.VoteGranted = false //default reply values
	reply.Term = rf.currTerm

	if args.Term >= rf.currTerm {

		// rf.logger.Printf("received requestVote from %v", args.CandidateId)

		//Respond valid if not given out vote or given out vote to candidate in same term
		//and candidate's log is up to date
		if (args.LastLogIdx >= len(rf.log)-1 && args.LastLogTerm >= rf.log[len(rf.log)-1].Term) &&
			(args.Term == rf.currTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) ||
			(args.Term > rf.currTerm) {

			// rf.logger.Printf("Granting requestVote to %v. resetting to follower on getting equiv or higher term.  them %v me %v", args.CandidateId, args.Term, rf.currTerm)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true

			rf.role = Follower
			rf.numVotes = 0
			rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
		}

		//Update term and reply's
		rf.currTerm = args.Term
		reply.Term = rf.currTerm

	}
	rf.mux.Unlock()
}

//
// sendRequestVote
// ===============
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {

	termSent := args.Term

	for {

		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

		rf.mux.Lock() //acquire for CS

		if rf.role != Candidate {
			// rf.logger.Printf("didn't check vote bc no longer candidate")
			rf.mux.Unlock()
			return
		}

		if termSent < rf.currTerm {
			// rf.logger.Printf("didn't check vote bc RPC was for old term %v, curr term %v", termSent, rf.currTerm)
			rf.mux.Unlock()
			return
		}

		if ok {
			// rf.logger.Printf("received Vote response from %v. granted status %v\n", server, reply.VoteGranted)

			//Higher candidate or leader. Reset to follower. Enter new term
			if rf.currTerm < reply.Term {
				// rf.logger.Printf("candidate resetting to follower after RPC response fro %v", server)
				rf.currTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.numVotes = 0
				rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
				rf.mux.Unlock()
				return
			}

			if reply.VoteGranted {
				rf.numVotes++
			}

			//become leader if gotten majority
			if rf.numVotes > len(rf.peers)/2 {
				rf.role = Leader
				// rf.logger.Printf("became leader")

				//reset all log next indices to leader's last entry + 1
				rf.logger.Printf("new leader restting other servers' log idx to %v", len(rf.log))
				for i := range rf.nextIdx {
					if i != rf.me {
						rf.nextIdx[i] = len(rf.log)
					}
				}

				rf.mux.Unlock()

				rf.ExecuteHeartbeat(true)
				return
			}

			rf.mux.Unlock()
			return //No need for retries

		} else {
			rf.mux.Unlock()
		}
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// rf.logger.Printf("sent heartbeat to %v", server)

	rf.mux.Lock() //Acquire Lock for CS

	if ok {
		// rf.logger.Printf("received heartbeat response from %v", server)

		//Leader is stale. Must Become follower
		if reply.Term > rf.currTerm {
			// rf.logger.Printf("prev leader resetting to follower on receiving AppendEntries response from %v", server)
			rf.role = Follower

			//New term. Reset election state variables
			rf.currTerm = reply.Term
			rf.votedFor = -1
			rf.numVotes = 0
			rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
		}
	}

	rf.mux.Unlock()
}

//RPC AppendEntries Method Handler used for Heartbeats and Log Replication Commands
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mux.Lock()            //CS accessing peer DS variables
	reply.Term = rf.currTerm //default reply values
	reply.success = false

	rf.logger.Printf("received append entries with prevIdx %v prevTerm %v entries %v", args.PrevLogIdx, args.PrevLogTerm, args.Entries)
	if rf.currTerm <= args.Term {
		// rf.logger.Printf("received valid heartbeat from leader %v", args.LeaderId)
		rf.currTerm = args.Term
		reply.Term = rf.currTerm //update terms

		//Acknowledge higher current leader. Reset to follower
		rf.role = Follower
		rf.numVotes = 0
		rf.votedFor = -1
		rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
		// rf.logger.Printf("resetting to follower on getting heartbeat from %v \n", args.LeaderId)

	}

	//check if raft contains different entry at prevLogIdx
	if args.PrevLogIdx > 0 && (len(rf.log)-1 < args.PrevLogIdx || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) {
		rf.logger.Printf("append unsuccessful")

		rf.mux.Unlock()
		return
	}
	reply.success = true
	rf.logger.Printf("append successful request %v, reply %v", args, reply)

	//update log and replicate using leaders' entries
	if !args.IsHeartbeat {
		rf.log = rf.log[:args.PrevLogIdx+1]
		rf.log = append(rf.log, args.Entries...)

	}

	//Set commit idx to min(leaderCommit, lastEntry idx)
	if args.LeaderCommitIdx > rf.commitIdx {
		rf.commitIdx = args.LeaderCommitIdx
		lastIdx := len(rf.log) - 1
		if lastIdx < rf.commitIdx {
			rf.commitIdx = lastIdx
		}
	}

	rf.mux.Unlock()
}

//
// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this server is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
//
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {

	rf.mux.Lock()
	defer rf.mux.Unlock()

	index := len(rf.log)
	term := rf.currTerm
	isLeader := rf.role == Leader

	if isLeader {
		rf.logger.Printf("Leader received new put Command %v\n", command)
		rf.cmdCh <- command
	}

	return index, term, isLeader
}

//
// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Stop() {
	rf.logger.SetOutput(ioutil.Discard)
}

// applyCh
// =======
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages. You can assume the channel
// is consumed in a timely manner.
//
// Creates a New raft server with goroutines in the backgroung to handle elections
// heartbeats and log replication
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{
		peers:       peers,
		me:          me, //default values
		role:        Follower,
		currTerm:    0,
		votedFor:    -1,
		numVotes:    0,
		applyCh:     applyCh,
		lastApplied: 0,
		commitIdx:   0,
		matchIdx:    make([]int, len(peers), len(peers)), //init to zero values
		nextIdx:     make([]int, len(peers), len(peers)),
		log:         make([]logEntry, 0),
		cmdCh:       make(chan interface{}),
	}

	for i := range rf.nextIdx {
		rf.nextIdx[i] = 1 //init to last log idx = 0 + 1
	}

	rf.log = append(rf.log, logEntry{Term: -1}) //pad so indices start at 1

	if kEnableDebugLogs {
		// peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", strconv.Itoa(rf.me)+" ")
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, strconv.Itoa(rf.me)+" ", log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	//Start timer
	rf.elecTimer = time.NewTicker(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

	//Initialize Background goroutines
	go rf.ElectionRoutine()
	go rf.HeartBeatRoutine()
	go rf.commandRoutine()
	go rf.ApplyRoutine()

	return rf
}

//Periodically holds elections for Follower & Candidate peers
func (rf *Raft) ElectionRoutine() {
	for {
		rf.mux.Lock() //Acquire Lock to check state
		select {
		case <-rf.elecTimer.C:
			//reset timeout duration
			rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

			switch rf.role {
			case Follower:
				// rf.logger.Printf("New election. follower became candidate")

				//Become candidate if not heard from leader or given out vote
				rf.role = Candidate
				rf.currTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1
				rf.mux.Unlock()

				rf.ExecuteCandidate()

			//Retry in the event of a split vote
			case Candidate:
				// rf.logger.Printf("old candiate started new election")
				//update state
				rf.currTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1
				rf.mux.Unlock()

				rf.ExecuteCandidate()

				//No action
			case Leader:
				rf.mux.Unlock()
			}
		default:
			rf.mux.Unlock()
		}
	}
}

//Background routine for Leader to send heartbeats to other servers
func (rf *Raft) HeartBeatRoutine() {

	INTERVAL := 100
	OFFSET := 50

	for {
		rf.ExecuteHeartbeat(false)

		rf.mux.Lock()
		if rf.role == Leader {
			rf.mux.Unlock()
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(OFFSET)+INTERVAL))
		} else {
			rf.mux.Unlock()
		}
	}
}

//Sends RequestVote RPCs to other peers in parallel
func (rf *Raft) ExecuteCandidate() {

	//Lock while preparing RPC args. Release during RPC
	rf.mux.Lock()
	// rf.logger.Printf("acquired for making  req Vote RPC args")
	args := &RequestVoteArgs{
		Term:        rf.currTerm,
		CandidateId: rf.me,
		LastLogIdx:  len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log)-1].Term,
	}
	rf.mux.Unlock()

	//Send request vote to peer servers and parse response
	for i, _ := range rf.peers {
		if i != rf.me {
			reply := &RequestVoteReply{}

			go rf.sendRequestVote(i, args, reply)
		}
	}
}

//Sends AppendEntry heartbeats from raft to peer servers
func (rf *Raft) ExecuteHeartbeat(now bool) {

	//role modified elsewhere, no action, release lock
	rf.mux.Lock()
	if rf.role != Leader {
		rf.mux.Unlock()
		return
	}

	for i, _ := range rf.peers {
		if i != rf.me {

			//Make RPC args
			args := &AppendEntriesArgs{
				Term:            rf.currTerm,
				LeaderId:        rf.me,
				LeaderCommitIdx: rf.commitIdx,
				IsHeartbeat:     true,
			}
			args.PrevLogIdx = rf.nextIdx[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIdx].Term

			reply := &AppendEntriesReply{}
			go rf.sendHeartBeat(i, args, reply)
		}
	}

	rf.mux.Unlock()
}

//Routine for handling client append Requests
func (rf *Raft) commandRoutine() {

	for {
		select {
		case cmd := <-rf.cmdCh:

			rf.mux.Lock()
			rf.logger.Printf("leader routine received command %v", cmd)

			//Apply command to leader state machine
			newEntry := logEntry{
				Term: rf.currTerm,
				Cmd:  cmd,
			}
			rf.log = append(rf.log, newEntry)

			//Send to candidates and await response
			rf.numSuceeded = 0
			rf.executeAppendEntries()

			rf.mux.Unlock()
		}
	}
}

func (rf *Raft) executeAppendEntries() {

	for i := range rf.peers {
		if i != rf.me {
			//Make RPC arguments per server
			args := &AppendEntriesArgs{
				Term:            rf.currTerm,
				LeaderId:        rf.me,
				LeaderCommitIdx: rf.commitIdx,
				IsHeartbeat:     false,
			}

			args.PrevLogIdx = rf.nextIdx[i] - 1
			if args.PrevLogIdx != 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIdx].Term
			} else {
				args.PrevLogTerm = -1
			}
			args.Entries = rf.log[rf.nextIdx[i]:len(rf.log)]

			//Send in background
			go rf.sendAppendEntries(i, args)

		}
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {

	termSent := args.Term

	for {
		rf.logger.Printf("new append entries sent to %v", server)
		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

		rf.mux.Lock()
		//Discard response if no longer Leader
		if rf.role != Leader {
			// rf.logger.Printf("didn't check appendEntries response bc no longer leader")
			rf.mux.Unlock()
			return
		}

		//Discard response if term is old
		if termSent < rf.currTerm {
			// rf.logger.Printf("didn't check appendEntry bc was for old term %v, curr term %v", termSent, rf.currTerm)
			rf.mux.Unlock()
			return
		}

		if ok {

			//Leader is stale. Must Become follower
			if reply.Term > rf.currTerm {
				// rf.logger.Printf("prev leader resetting to follower on receiving AppendEntries response from %v", server)
				rf.role = Follower

				//New term. Reset election state variables
				rf.currTerm = reply.Term
				rf.votedFor = -1
				rf.numVotes = 0
				rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

				rf.mux.Unlock()
				return
			}

			rf.logger.Printf("Append entry reply state from %v %v", server, reply.success)
			if reply.success {
				rf.numSuceeded++

				//update server's idx variables
				rf.nextIdx[server] = rf.nextIdx[server] + len(args.Entries)
				rf.matchIdx[server] = rf.nextIdx[server] - 1

				//fast forward commit idx if majority Append successes recieved
				if rf.numSuceeded+1 > len(rf.peers)/2 {
					curr := rf.commitIdx
					for i := curr + 1; i < len(rf.log); i++ {

						if rf.log[i].Term != rf.currTerm { //new commit idx term must be current
							continue
						}
						//majority with match Idx at least target commmit Idx
						numCount := 0
						for _, peerIdx := range rf.matchIdx {
							if peerIdx != rf.me && peerIdx >= i {
								numCount++
							}
						}
						if numCount+1 > len(rf.peers)/2 {
							rf.commitIdx = i
						}
					}
				}

				rf.mux.Unlock()
				return //no need for retries

			} else {
				rf.nextIdx[server]--                     //decrement nextIdx.
				args.PrevLogIdx = rf.nextIdx[server] - 1 //  Update arguments.
				args.PrevLogTerm = rf.log[args.PrevLogIdx].Term
				args.LeaderCommitIdx = rf.commitIdx

				rf.mux.Unlock()
				continue //Retry till successful
			}

		} else {
			rf.mux.Unlock() //No response. retries needed
		}

	}
}

func (rf *Raft) ApplyRoutine() {

	for {
		rf.mux.Lock()
		if rf.lastApplied < rf.commitIdx {
			toApply := rf.log[rf.lastApplied : rf.commitIdx+1]
			applyStart := rf.lastApplied + 1
			rf.lastApplied = rf.commitIdx

			rf.mux.Unlock()
			go rf.ApplyEntries(toApply, applyStart) //apply in background

		} else {
			rf.mux.Unlock()
		}

		//Apply at intervals. Prevent waste of resources
		time.Sleep(time.Duration(time.Millisecond * 150))
	}
}

func (rf *Raft) ApplyEntries(toApply []logEntry, start int) {

	for i, item := range toApply {
		cmd := ApplyCommand{Index: start + i, Command: item.Cmd}
		rf.applyCh <- cmd
	}
}
