package mule

import (
	"errors"
	"sync"
)

var ErrQueueMaxLimit = errors.New("queue max limit reached")

// SyncQueue thread safety queue, Get method blocks when queue is empty
type SyncQueue struct {
	hasNodes *sync.Cond
	nodes    [][]byte
	maxSize  int
	// use this channel to close this queue
	close chan struct{}
}

func NewQueue(maxSize int) *SyncQueue {
	return &SyncQueue{
		hasNodes: sync.NewCond(&sync.Mutex{}),
		nodes:    make([][]byte, 0),
		maxSize:  maxSize,
		close:    make(chan struct{}),
	}
}

func (q *SyncQueue) Get() ([]byte, bool) {
	q.hasNodes.L.Lock()
	defer q.hasNodes.L.Unlock()

	for len(q.nodes) == 0 {
		select {
		case <-q.close:
			return nil, false
		default:
			q.hasNodes.Wait()
		}
	}
	return q.nodes[0], true
}

func (q *SyncQueue) Take() ([]byte, bool) {
	el, ok := q.Get()
	q.Remove()
	return el, ok
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

func (q *SyncQueue) Remove() {
	q.hasNodes.L.Lock()
	defer q.hasNodes.L.Unlock()
	if len(q.nodes) > 0 {
		q.nodes = q.nodes[1:]
	}
}

// Close queue manually to unlock goroutine
func (q *SyncQueue) Close() {
	close(q.close)
	q.hasNodes.Broadcast()
}
