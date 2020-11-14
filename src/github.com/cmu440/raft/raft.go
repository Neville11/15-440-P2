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
	numVotes    int          //votes granted
	numReceived int          //RPCs received
	logger      *log.Logger  // We provide you with a separate logger per peer.
	elecTimer   *time.Ticker //ticker for election Timeouts

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

// RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term        int
	LeaderId    int
	isHeartbeat bool
}

type AppendEntriesReply struct {
	Term int
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	rf.mux.Lock() //CS accessing raft DS variables
	// rf.logger.Printf("aquired lock for RPC response")

	reply.VoteGranted = false //default reply values
	reply.Term = rf.currTerm

	if args.Term >= rf.currTerm {

		rf.logger.Printf("received requestVote from %v", args.CandidateId)

		//Respond valid if not given out vote or given out vote to candidate in same term
		if (args.Term == rf.currTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId)) ||
			(args.Term > rf.currTerm) {

			rf.logger.Printf("Granting requestVote to %v. resetting to follower on getting equiv or higher term.  them %v me %v", args.CandidateId, args.Term, rf.currTerm)
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
			rf.logger.Printf("didn't check vote bc no longer candidate")
			rf.mux.Unlock()
			return
		}

		if termSent < rf.currTerm {
			rf.logger.Printf("didn't check vote bc RPC was for old term %v, curr term %v", termSent, rf.currTerm)
			rf.mux.Unlock()
			return
		}

		if ok {
			rf.logger.Printf("received Vote response from %v. granted status %v\n", server, reply.VoteGranted)

			//Higher candidate or leader. Reset to follower. Enter new term
			if rf.currTerm < reply.Term {
				rf.logger.Printf("candidate resetting to follower after RPC response fro %v", server)
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
				rf.logger.Printf("became leader")
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

	termSent := args.Term

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.logger.Printf("sent heartbeat to %v", server)

	rf.mux.Lock() //Acquire Lock for CS

	if termSent != rf.currTerm {
		rf.logger.Printf("Ignoring heartbeat response from %v as term is old %v compared to %v", server, termSent, rf.currTerm)
		rf.mux.Unlock()
		return
	}

	//Leader is stale. Must Become follower
	if ok {
		rf.logger.Printf("received heartbeat response from %v", server)

		if reply.Term > rf.currTerm {
			rf.logger.Printf("prev leader resetting to follower on receiving AppendEntries response from %v", server)
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

	rf.mux.Lock()            //CS accessing raft DS variables
	reply.Term = rf.currTerm //default reply values

	if rf.currTerm <= args.Term {
		rf.logger.Printf("received valid heartbeat from leader %v", args.LeaderId)
		rf.currTerm = args.Term
		reply.Term = rf.currTerm //update terms

		//Acknowledge higher current leader. Reset to follower
		rf.role = Follower
		rf.numVotes = 0
		rf.votedFor = -1
		rf.elecTimer.Reset(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)
		rf.logger.Printf("resetting to follower on getting heartbeat from %v \n", args.LeaderId)

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B)

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
		peers:    peers,
		me:       me, //default values
		role:     Follower,
		currTerm: 0,
		votedFor: -1,
		numVotes: 0,
	}

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

	rf.elecTimer = time.NewTicker(time.Duration(rand.Intn(RANGE)+LOWER) * time.Millisecond)

	//Initialize Background goroutines
	go rf.ElectionRoutine()
	go rf.HeartBeatRoutine()

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
				rf.logger.Printf("New election. follower became candidate")

				//Become candidate if not heard from leader or given out vote
				rf.role = Candidate
				rf.currTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1
				rf.numReceived = 0
				rf.mux.Unlock()

				rf.ExecuteCandidate()

			//Retry in the event of a split vote
			case Candidate:
				rf.logger.Printf("old candiate started new election")
				//update state
				rf.currTerm++
				rf.votedFor = rf.me
				rf.numVotes = 1
				rf.numReceived = 0
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
	rf.logger.Printf("acquired for making  req Vote RPC args")
	args := &RequestVoteArgs{
		Term:        rf.currTerm,
		CandidateId: rf.me,
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

	//Make RPC args
	args := &AppendEntriesArgs{
		Term:        rf.currTerm,
		LeaderId:    rf.me,
		isHeartbeat: true,
	}

	if !now {
		rf.mux.Unlock()
	}

	for i, _ := range rf.peers {
		if i != rf.me {

			reply := &AppendEntriesReply{}
			go rf.sendHeartBeat(i, args, reply)
		}
	}

	if now {
		rf.mux.Unlock()
	}
}
