package mule

import (
	"errors"
	"sync"
)

var ErrQueueMaxLimit = errors.New("queue max limit reached")

type SyncQueue struct {
	cond    *sync.Cond
	nodes   [][]byte
	maxSize int
}

func NewQueue(maxSize int) *SyncQueue {
	return &SyncQueue{
		cond:    sync.NewCond(&sync.Mutex{}),
		nodes:   make([][]byte, 0),
		maxSize: maxSize,
	}
}

func (q *SyncQueue) Get() []byte {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if len(q.nodes) == 0 {
		q.cond.Wait()
	}
	return q.nodes[0]
}

func (q *SyncQueue) Take() []byte {
	node := q.Get()

	q.cond.L.Lock()
	q.nodes = q.nodes[1:]
	q.cond.L.Unlock()

	return node
}

func (q *SyncQueue) Add(node []byte) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if len(q.nodes) == q.maxSize {
		return ErrQueueMaxLimit
	}
	q.nodes = append(q.nodes, node)
	q.cond.Signal()
	return nil
}
