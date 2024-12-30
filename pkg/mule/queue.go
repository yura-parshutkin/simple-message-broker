package mule

import (
	"errors"
	"sync"
)

var ErrQueueMaxLimit = errors.New("queue max limit reached")

type SyncQueue struct {
	hasNodes *sync.Cond
	nodes    [][]byte
	maxSize  int
}

func NewQueue(maxSize int) *SyncQueue {
	return &SyncQueue{
		hasNodes: sync.NewCond(&sync.Mutex{}),
		nodes:    make([][]byte, 0),
		maxSize:  maxSize,
	}
}

func (q *SyncQueue) Get() []byte {
	q.hasNodes.L.Lock()
	defer q.hasNodes.L.Unlock()

	for len(q.nodes) == 0 {
		q.hasNodes.Wait()
	}
	return q.nodes[0]
}

func (q *SyncQueue) Take() []byte {
	q.hasNodes.L.Lock()
	defer q.hasNodes.L.Unlock()

	for len(q.nodes) == 0 {
		q.hasNodes.Wait()
	}
	n := q.nodes[0]
	q.nodes = q.nodes[1:]
	return n
}

func (q *SyncQueue) Add(node []byte) error {
	q.hasNodes.L.Lock()
	defer q.hasNodes.L.Unlock()

	if len(q.nodes) == q.maxSize {
		return ErrQueueMaxLimit
	}
	q.nodes = append(q.nodes, node)
	q.hasNodes.Signal()
	return nil
}
