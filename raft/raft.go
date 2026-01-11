// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peers []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	hs, _, _ := c.Storage.InitialState()

	log := newLog(c.Storage)
	log.committed = hs.Commit
	log.applied = c.Applied

	r := Raft{
		id:               c.ID,
		Term:             hs.Term,
		Vote:             hs.Vote,
		Lead:             None,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		State:            StateFollower,
		RaftLog:          log,
		peers:            c.peers,
	}
	r.becomeFollower(hs.Term, None)
	r.RaftLog.committed = hs.Commit
	r.Vote = hs.Vote

	return &r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	prevLogIndex := r.Prs[to].Next - 1
	var prevLogTerm uint64
	if prevLogIndex == 0 {
		prevLogTerm = 0
	} else {
		var err error
		prevLogTerm, err = r.RaftLog.Term(prevLogIndex)
		if err != nil {
			panic(err)
		}
	}

	li := r.RaftLog.LastIndex()
	var ents []*pb.Entry
	if li >= r.Prs[to].Next {
		slice := r.RaftLog.slice(r.Prs[to].Next, li+1)
		ents = make([]*pb.Entry, len(slice))
		for i := range slice {
			ents[i] = &slice[i]
		}
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Entries: ents,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Commit:  r.RaftLog.committed,
	}

	r.msgs = append(r.msgs, m)

	return true
}

func (r *Raft) bcastAppend() {
	r.visitPeers(func(to uint64) {
		r.sendAppend(to)
	})
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	commit := r.RaftLog.committed
	if pr, ok := r.Prs[to]; ok && pr.Match < commit {
		commit = pr.Match
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    r.Term,
		From:    r.id,
		To:      to,
		Commit:  commit,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From:    r.id,
				To:      r.id,
			})
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,
				To:      r.id,
			})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term = term
	r.State = StateFollower
	r.Vote = None
	r.Lead = lead
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.Term++
	r.State = StateCandidate
	r.Vote = r.id
	r.Lead = None
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Vote = None
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)

	r.Prs = make(map[uint64]*Progress)
	for _, peer := range r.peers {
		r.Prs[peer] = &Progress{
			Next:  r.RaftLog.LastIndex() + 1,
			Match: 0,
		}
		if r.id == peer {
			r.Prs[peer].Match = r.RaftLog.LastIndex()
		}
	}

	li := r.RaftLog.LastIndex()
	r.appendEntry(&pb.Entry{
		Term:  r.Term,
		Index: li + 1,
		Data:  nil,
	})
	r.bcastAppend()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLedaer(m)
	}

	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		if len(r.peers) == 1 {
			r.becomeLeader()
		} else {
			r.campaign()
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHup:
		if len(r.peers) == 1 {
			r.becomeLeader()
		} else {
			r.becomeCandidate()
			r.campaign()
		}
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject

		quorum := (len(r.peers) / 2) + 1
		winCount := 0
		rejCount := 0
		for _, vote := range r.votes {
			if vote {
				winCount++
			} else {
				rejCount++
			}
		}

		if winCount >= quorum {
			r.becomeLeader()
		} else if rejCount >= quorum {
			r.becomeFollower(r.Term, None)
		}
	}
	return nil
}

func (r *Raft) campaign() {
	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm := r.RaftLog.MustTerm(lastLogIndex)
	r.visitPeers(func(to uint64) {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			Term:    r.Term,
			From:    r.id,
			To:      to,
			Index:   lastLogIndex,
			LogTerm: lastLogTerm,
		})
	})
}

func (r *Raft) stepLedaer(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.visitPeers(func(to uint64) {
			r.sendHeartbeat(to)
		})
	case pb.MessageType_MsgPropose:
		r.appendEntry(m.Entries...)
		r.bcastAppend()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeatResponse:
		if m.Reject {
		} else if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
	return nil
}

func (r *Raft) visitPeers(h func(uint64)) {
	for _, peer := range r.peers {
		if peer == r.id {
			continue
		}
		h(peer)
	}
}

func (r *Raft) canVoteFor(m pb.Message) bool {
	if m.Term < r.Term {
		return false
	}

	if r.Vote != None && r.Vote != m.From {
		return false
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm := r.RaftLog.MustTerm(lastLogIndex)

	return lastLogTerm < m.LogTerm || (lastLogTerm == m.LogTerm && m.Index >= lastLogIndex)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	canVoteFor := r.canVoteFor(m)
	if canVoteFor {
		r.Vote = m.From
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Reject:  !canVoteFor,
	})
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}
	r.becomeFollower(m.Term, m.From)

	term, err := r.RaftLog.Term(m.Index)
	if err != nil {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	if term != m.LogTerm {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			Term:    r.Term,
			From:    r.id,
			To:      m.From,
			Reject:  true,
		})
		return
	}

	ents := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		if term, err := r.RaftLog.Term(ent.Index); err == nil && term == ent.Term {
			continue
		}
		ents = append(ents, *ent)
	}

	li := r.RaftLog.truncateAppend(ents...)

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Index:   li,
	})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}

	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = r.Prs[m.From].Match + 1
	}

	match := make([]uint64, 0, len(r.peers))
	for _, pr := range r.Prs {
		match = append(match, pr.Match)
	}
	sort.Slice(match, func(i, j int) bool {
		return match[i] > match[j]
	})

	quorum := len(r.peers) / 2
	committed := match[quorum]

	if committed > r.RaftLog.committed {
		term, _ := r.RaftLog.Term(committed)
		if term == r.Term {
			r.RaftLog.committed = committed
			r.bcastAppend()
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	if m.Term < r.Term {
		return
	}

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = m.Commit
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    r.Term,
		From:    r.id,
		To:      m.From,
		Commit:  r.RaftLog.committed,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) appendEntry(es ...*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()

	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + 1 + uint64(i)
	}

	entries := make([]pb.Entry, len(es))
	for i := range es {
		entries[i] = *es[i]
	}

	li := r.RaftLog.append(entries...)

	if r.State == StateLeader {
		r.Prs[r.id].Match = li
		r.Prs[r.id].Next = r.Prs[r.id].Match + 1

		if len(r.peers) == 1 {
			r.RaftLog.committed = li
		}
	}
}

func (r *Raft) softState() SoftState { return SoftState{Lead: r.Lead, RaftState: r.State} }

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
