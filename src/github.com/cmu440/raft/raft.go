// raft.go
// =======
// Implements Google's Raft algorithim

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
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

var RANGE, LOWER = 250, 800 //range used for election Intervals

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
// ===========ß
// A Go object implementing a single Raft peer
type Raft struct {
	mux         sync.Mutex       // Lock to protect shared access to this peer's state
	peers       []*rpc.ClientEnd // RPC end points of all peers
	me          int              // this peer's index into peers[]
	role        RoleType
	currTerm    int
	votedFor    int
	numVotes    int          //votes acquired
	logger      *log.Logger  // We provide you with a separate logger per peer.
	elecTimer   *time.Ticker //ticker for election Timeouts
	applyCh     chan ApplyCommand
	commitIdx   int   //idx of highest committed entry
	lastApplied int   //idx of highest entry applied to state machine
	nextIdx     []int //idx of next log entry to send per server
	matchIdx    []int //idx of highest entry known to be replicated per server
	log         []logEntry
	backlog     []interface{} //log entries to be later committed
}

//
// GetState()
// ==========
// Returns "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	rf.mux.Lock() //CS for accessing state variables
	defer rf.mux.Unlock()

	me, term := rf.me, rf.currTerm
	isleader := rf.role == Leader

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

// Handles RequestVote RPC from candidate
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mux.Lock() //CS accessing raft DS variables
	defer rf.mux.Unlock()

	reply.VoteGranted = false //default reply values
	reply.Term = rf.currTerm

	if args.Term >= rf.currTerm {

		//Respond yes if not given out vote or given out vote to candidate in same term
		//and candidate's log is up to date
		lastIdx := len(rf.log) - 1
		if (args.LastLogTerm > rf.log[lastIdx].Term ||
			(args.LastLogTerm == rf.log[lastIdx].Term && args.LastLogIdx >= lastIdx)) &&
			((args.Term == rf.currTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) ||
				(args.Term > rf.currTerm)) {

			rf.votedFor = args.CandidateId
			reply.VoteGranted = true

			rf.role = Follower //reset to follower
			rf.numVotes = 0
			rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
		}

		//Update term and reply's
		rf.currTerm = args.Term
		reply.Term = rf.currTerm
	}
}

//Sends a non-blocking requestVote RPC from a candidate to a single server and handles
//the response
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {

	termSent := args.Term

	for {

		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

		rf.mux.Lock() //acquire for CS

		if rf.role != Candidate { //discard if no longer candidate
			rf.mux.Unlock()
			return
		}

		// Discard if old term. Elected, New election started, or became follower
		if termSent < rf.currTerm {
			rf.mux.Unlock()
			return
		}

		if ok {

			//Higher candidate or leader. Reset to follower. Enter new term
			if rf.currTerm < reply.Term {
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
				rf.backlog = rf.backlog[:0] //clear backlog

				//reset all log next indices to leader's last log Idx + 1
				for i := range rf.nextIdx {
					if i != rf.me {
						rf.nextIdx[i] = len(rf.log)
					}
				}

				rf.ExecuteHeartbeat()
				rf.mux.Unlock()
				return
			}
			rf.mux.Unlock()
			return //No need for retries

		} else {
			args.LastLogIdx = len(rf.log) - 1 //update arguments and retry
			args.LastLogTerm = rf.log[len(rf.log)-1].Term
			rf.mux.Unlock()
		}
	}
}

//Sends non-blocking RPC Heartbeat to follower, waits and parses response
func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mux.Lock() //Acquire Lock for CS

	if ok {
		//Leader is stale. Must Become follower
		if reply.Term > rf.currTerm {
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

	rf.mux.Lock() //CS accessing peer DS variables
	defer rf.mux.Unlock()

	reply.Term = rf.currTerm //default reply values
	reply.Success = false

	if rf.currTerm <= args.Term {
		rf.currTerm = args.Term
		reply.Term = rf.currTerm //update terms

		//Acknowledge higher current leader. Reset to follower
		rf.role = Follower
		rf.numVotes = 0
		rf.votedFor = -1
		rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

	} else {
		return
	}

	//check if raft contains different entry at prevLogIdx
	if args.PrevLogIdx > 0 &&
		(len(rf.log)-1 < args.PrevLogIdx || rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) {
		return
	}
	reply.Success = true

	//update log and replicate using leaders' entries
	if !args.IsHeartbeat {

		rf.log = rf.log[:args.PrevLogIdx+1]
		rf.log = append(rf.log, args.Entries...)

	}

	//Set commit idx to min(leaderCommit, lastEntry idx)
	if args.LeaderCommitIdx > rf.commitIdx {

		//update commitIdx
		rf.commitIdx = args.LeaderCommitIdx
		lastIdx := len(rf.log) - 1
		if lastIdx < rf.commitIdx {
			rf.commitIdx = lastIdx
		}
	}
}

// PutCommand
// =====
//  Starts a command  agreement if the server is a leader and return immediately
// The first return value is the index that the command will appear at
// if it is ever committed
// The second return value is the peer's current term
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {

	rf.mux.Lock()
	defer rf.mux.Unlock()

	index := len(rf.log) + len(rf.backlog)
	term := rf.currTerm
	isLeader := rf.role == Leader

	if isLeader {
		rf.backlog = append(rf.backlog, command)
	}

	return index, term, isLeader
}

// Stop
// ====
//Turns off a peers debug output
func (rf *Raft) Stop() {
	rf.logger.SetOutput(ioutil.Discard)
}

// Creates a New server and associated data structures
// with goroutines in the backgroung to handle elections log replication
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
		backlog:     make([]interface{}, 0),
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
		select {
		case <-rf.elecTimer.C:
			rf.mux.Lock() //Acquire Lock to check state

			//reset timeout duration
			rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

			//Retry if did not win or follower not heard from leader
			if rf.role == Candidate || rf.role == Follower {

				rf.role = Candidate
				rf.currTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1

				rf.ExecuteCandidate()
			}
			rf.mux.Unlock()
		}
	}
}

//Background routine for Leader to periodically
// send heartbeats to other servers
func (rf *Raft) HeartBeatRoutine() {

	INTERVAL := 100

	for {
		rf.mux.Lock()

		if rf.role == Leader {
			rf.ExecuteHeartbeat()
		}
		rf.mux.Unlock() //role modified elsewhere, no action

		time.Sleep(time.Millisecond * time.Duration(INTERVAL)) //Send at intervals
	}
}

//Sends RequestVote RPCs to other peers in parallel
func (rf *Raft) ExecuteCandidate() {

	//Create RPC args, and spawn goroutines to send request and parse responses
	for i, _ := range rf.peers {
		if i != rf.me {
			args := &RequestVoteArgs{
				Term:        rf.currTerm,
				CandidateId: rf.me,
				LastLogIdx:  len(rf.log) - 1,
				LastLogTerm: rf.log[len(rf.log)-1].Term,
			}

			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

//Sends AppendEntry heartbeats from raft to peer servers
func (rf *Raft) ExecuteHeartbeat() {

	for i, _ := range rf.peers {
		if i != rf.me {

			//Make RPC args
			args := &AppendEntriesArgs{
				Term:     rf.currTerm,
				LeaderId: rf.me,
				// LeaderCommitIdx: rf.commitIdx,
				IsHeartbeat: true,
			}
			args.PrevLogIdx = rf.nextIdx[i] - 1
			args.PrevLogTerm = rf.log[args.PrevLogIdx].Term

			args.LeaderCommitIdx = rf.commitIdx
			if args.PrevLogIdx < args.LeaderCommitIdx {
				args.LeaderCommitIdx = args.PrevLogIdx
			}

			reply := &AppendEntriesReply{}
			go rf.sendHeartBeat(i, args, reply)
		}
	}
}

//Routine for initiating client commit Requests
func (rf *Raft) commandRoutine() {
	INTERVAL := 50

	for {
		rf.mux.Lock()
		if rf.role == Leader && len(rf.backlog) > 0 {
			//pop cmd from backlog
			cmd := rf.backlog[0]
			rf.backlog = rf.backlog[1:]

			//Apply command to leader state machine
			newEntry := logEntry{
				Term: rf.currTerm,
				Cmd:  cmd,
			}
			rf.log = append(rf.log, newEntry)

			//Send to followers to replicate and await response
			rf.executeAppendEntries(newEntry)

			rf.mux.Unlock()
		} else {
			rf.mux.Unlock()
			time.Sleep(time.Millisecond * time.Duration(INTERVAL)) //prevent waste of CPU resources
		}
	}
}

//Makes AppendEntries RPC args and spawns routines to send RPC from
//Leader to followers in parallel,
func (rf *Raft) executeAppendEntries(newEntry logEntry) {

	for i := range rf.peers {
		if i != rf.me {
			//Make RPC arguments per server
			args := &AppendEntriesArgs{
				Term:        rf.currTerm,
				LeaderId:    rf.me,
				IsHeartbeat: false,
			}

			args.PrevLogIdx = rf.nextIdx[i] - 1
			if args.PrevLogIdx != 0 { //prevent idx out of range for initial value
				args.PrevLogTerm = rf.log[args.PrevLogIdx].Term
			} else {
				args.PrevLogTerm = -1
			}

			args.LeaderCommitIdx = rf.commitIdx
			//edge case, sent commit Idx shouldn't be higher than matchIdx
			if args.PrevLogIdx < args.LeaderCommitIdx {
				args.LeaderCommitIdx = args.PrevLogIdx
			}
			args.Entries = rf.log[args.PrevLogIdx+1:]
			logLength := len(rf.log)

			//Send in background
			go rf.sendAppendEntries(i, args, logLength)
		}
	}
}

//Sends non-blocking AppendEntries from a Leader to Followers and handles responses
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, initLogLength int) {

	termSent := args.Term

	for {
		//Don't send if new PutCommand, RPC called later will handle
		rf.mux.Lock()
		if len(rf.log) > initLogLength {
			rf.mux.Unlock()
			return
		}
		rf.mux.Unlock()

		reply := &AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

		rf.mux.Lock()
		if rf.role != Leader { //Discard response if no longer Leader
			rf.mux.Unlock()
			return
		}

		if termSent < rf.currTerm { //Discard response if term is old
			rf.mux.Unlock()
			return
		}

		if ok {
			//Leader is stale. Must Become follower
			if reply.Term > rf.currTerm {
				rf.role = Follower

				//New term. Reset election state variables
				rf.currTerm = reply.Term
				rf.votedFor = -1
				rf.numVotes = 0
				rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

				rf.mux.Unlock()
				return
			}

			if len(rf.log) > initLogLength { //Don't parse response, newer RPC will handle
				rf.mux.Unlock()
				return
			}

			if reply.Success {
				//update server's idx variables
				rf.nextIdx[server] = rf.nextIdx[server] + len(args.Entries)
				rf.matchIdx[server] = rf.nextIdx[server] - 1

				//fast forward commit idx if majority Append successes recieved
				old := rf.commitIdx
				for i := old + 1; i < len(rf.log); i++ {

					if rf.log[i].Term != rf.currTerm { //new commit idx term must be for currTerm
						continue
					}
					//majority with match Idx at least target commmit Idx
					numCount := 0
					for who, peerIdx := range rf.matchIdx {
						if who != rf.me && peerIdx >= i {
							numCount++
						}
					}
					if numCount+1 > len(rf.peers)/2 {
						rf.commitIdx = i
					}
				}

				rf.mux.Unlock()
				return //no need for retries
			} else {
				rf.nextIdx[server]--                     //decrement nextIdx.
				args.PrevLogIdx = rf.nextIdx[server] - 1 //  Update arguments.
				args.PrevLogTerm = rf.log[args.PrevLogIdx].Term

				args.LeaderCommitIdx = rf.commitIdx
				if args.PrevLogIdx < args.LeaderCommitIdx {
					args.LeaderCommitIdx = args.PrevLogIdx
				}
				args.Entries = rf.log[args.PrevLogIdx+1:]

				rf.mux.Unlock()
				continue //Retry till successful
			}
		} else {
			args.PrevLogIdx = rf.nextIdx[server] - 1 //  Update arguments.
			args.PrevLogTerm = rf.log[args.PrevLogIdx].Term

			args.LeaderCommitIdx = rf.commitIdx
			if args.PrevLogIdx < args.LeaderCommitIdx {
				args.LeaderCommitIdx = args.PrevLogIdx
			}
			args.Entries = rf.log[args.PrevLogIdx+1:]
			rf.mux.Unlock() //No response. retries needed
		}
	}
}

//Periodically applies new committed log entries to state machine
func (rf *Raft) ApplyRoutine() {

	INTERVAL := 50
	for {
		rf.mux.Lock()
		if rf.lastApplied < rf.commitIdx {
			toApply := rf.log[rf.lastApplied+1 : rf.commitIdx+1]

			applyStart := rf.lastApplied + 1
			rf.lastApplied = rf.commitIdx

			rf.mux.Unlock()
			go rf.ApplyEntries(toApply, applyStart) //apply in background

		} else {
			rf.mux.Unlock()
			//Apply at intervals. Prevent waste of resources
			time.Sleep(time.Duration(INTERVAL) * time.Millisecond)
		}
	}
}

//Applies given entries to raft's Apply Channel
func (rf *Raft) ApplyEntries(toApply []logEntry, start int) {

	for i, item := range toApply {
		cmd := ApplyCommand{Index: start + i,
			Command: item.Cmd}
		rf.applyCh <- cmd
	}
}
