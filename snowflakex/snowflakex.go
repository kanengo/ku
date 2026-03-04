package snowflakex

import (
	"errors"
	"sync"
	"time"
)

const (
	// WorkerIDBits allocates bits for worker id
	WorkerIDBits = 10
	// SequenceBits allocates bits for sequence
	SequenceBits = 12

	// MaxWorkerID is the max value for worker id
	MaxWorkerID = -1 ^ (-1 << WorkerIDBits)
	// MaxSequence is the max value for sequence
	MaxSequence = -1 ^ (-1 << SequenceBits)

	// WorkerIDShift shift count for worker id
	WorkerIDShift = SequenceBits
	// TimestampShift shift count for timestamp
	TimestampShift = SequenceBits + WorkerIDBits

	// DefaultEpoch is the default start time (2020-01-01 00:00:00 UTC)
	DefaultEpoch = 1577836800000
)

// noCopy may be embedded into structs which must not be copied
// after the first use.
//
// See https://golang.org/issues/8005#issuecomment-190753527
// for details.
type noCopy struct{}

// Lock is a no-op used by -copylocks checker from `go vet`.
func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Node represents a snowflake node that generates IDs
type Node struct {
	noCopy    noCopy
	mu        sync.Mutex
	timestamp int64
	workerID  int64
	sequence  int64
	epoch     int64
}

// NewNode creates a new snowflake node
// workerID: 0-1023
// epoch: base timestamp in milliseconds (e.g., 1577836800000 for 2020-01-01)
func NewNode(workerID int64, epoch int64) (*Node, error) {
	if workerID < 0 || workerID > MaxWorkerID {
		return nil, errors.New("worker ID excess of max worker ID")
	}

	if epoch == 0 {
		epoch = DefaultEpoch
	}

	return &Node{
		timestamp: 0,
		workerID:  workerID,
		sequence:  0,
		epoch:     epoch,
	}, nil
}

// Generate creates and returns a unique snowflake ID
func (n *Node) Generate() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	current := time.Now().UnixMilli()

	if current < n.timestamp {
		// Clock moved backwards, refuse to generate id
		// For simplicity, we wait until the clock catches up
		// In production, you might want to return an error if the drift is too large
		for current <= n.timestamp {
			current = time.Now().UnixMilli()
		}
	}

	if n.timestamp == current {
		n.sequence = (n.sequence + 1) & MaxSequence
		if n.sequence == 0 {
			// Sequence Overflow, wait for next millisecond
			for current <= n.timestamp {
				current = time.Now().UnixMilli()
			}
		}
	} else {
		n.sequence = 0
	}

	n.timestamp = current

	return ((current - n.epoch) << TimestampShift) | (n.workerID << WorkerIDShift) | n.sequence
}

// GetTimeFromID returns the timestamp in milliseconds from the ID
func (n *Node) GetTimeFromID(id int64) int64 {
	return (id >> TimestampShift) + n.epoch
}

// GetTime returns the time.Time from the ID
func (n *Node) GetTime(id int64) time.Time {
	return time.UnixMilli(n.GetTimeFromID(id))
}
