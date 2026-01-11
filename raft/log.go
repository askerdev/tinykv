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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

var (
	ErrOutOfRange = errors.New("index out of range")
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hs, _, _ := storage.InitialState()

	return &RaftLog{
		storage:   storage,
		committed: hs.Commit,
		applied:   firstIndex - 1,
		stabled:   lastIndex,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	return l.slice(l.firstIndex(), l.LastIndex()+1)
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.slice(l.stabled+1, l.LastIndex()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	return l.slice(l.applied+1, l.committed+1)
}

func (l *RaftLog) slice(lo, hi uint64) []pb.Entry {
	if lo == hi {
		return make([]pb.Entry, 0)
	}

	if lo > hi {
		panic(ErrOutOfRange)
	}

	var offset uint64
	if len(l.entries) > 0 {
		offset = l.entries[0].Index
	} else {
		ents, _ := l.storage.Entries(lo, hi)
		dst := make([]pb.Entry, len(ents))
		copy(dst, ents)
		return dst
	}

	// Full in memory slice
	if lo >= offset {
		src := l.entries[lo-offset : hi-offset]
		dst := make([]pb.Entry, len(src))
		copy(dst, src)
		return dst
	}

	// Full in storage slice
	if hi < offset {
		ents, _ := l.storage.Entries(lo, hi)
		dst := make([]pb.Entry, len(ents))
		copy(dst, ents)
		return dst
	}

	// Part in both
	stored, _ := l.storage.Entries(lo, offset)

	stored = append(stored, l.entries[0:hi-offset]...)

	dst := make([]pb.Entry, len(stored))
	copy(dst, stored)

	return dst
}

func (l *RaftLog) firstIndex() uint64 {
	firstIndex, err := l.storage.FirstIndex()
	if err != nil && len(l.entries) > 0 {
		return l.entries[0].Index
	}
	return firstIndex
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if length := len(l.entries); length > 0 {
		return l.entries[length-1].Index
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if length := uint64(len(l.entries)); length > 0 && l.entries[0].Index <= i && i <= l.entries[length-1].Index {
		firstIndex := l.entries[0].Index
		return l.entries[i-firstIndex].Term, nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) MustTerm(i uint64) uint64 {
	term, err := l.Term(i)
	if err != nil {
		panic(err)
	}
	return term
}

func (l *RaftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	l.entries = append(l.entries, ents...)

	return l.LastIndex()
}

func (l *RaftLog) truncateAppend(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}

	var offset uint64
	if len(l.entries) > 0 {
		offset = l.entries[0].Index
	}
	fromIndex := ents[0].Index

	switch {
	case fromIndex == offset+uint64(len(l.entries)):
		l.entries = append(l.entries, ents...)
	case len(l.entries) == 0:
		l.entries = ents
	default:
		keep := l.slice(offset, fromIndex)
		l.entries = append(keep, ents...)
	}

	l.stabled = min(l.stabled, fromIndex-1)

	return l.LastIndex()
}
