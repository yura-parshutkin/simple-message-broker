package mule

import (
	"context"
	"errors"
	"sync"
)

var ErrSubsMaxLimit = errors.New("subs max limit reached")
var ErrQueueNotFound = errors.New("queue not found")

type Broker struct {
	qw map[string]*QueueWorker
}

func NewBroker(config Config) *Broker {
	w := make(map[string]*QueueWorker, len(config))
	for _, c := range config {
		subs := make([]chan []byte, 0, c.SubsSize)
		queue := NewQueue(c.Size)
		w[c.QueueName] = NewQueueWorker(subs, queue)
	}
	return &Broker{qw: w}
}

func (b *Broker) AddMessage(ctx context.Context, queue string, msg []byte) error {
	w, ok := b.qw[queue]
	if !ok {
		return ErrQueueNotFound
	}
	return w.AddMessage(ctx, msg)
}

func (b *Broker) Subscribe(ctx context.Context, queue string) (<-chan []byte, error) {
	w, ok := b.qw[queue]
	if !ok {
		return nil, ErrQueueNotFound
	}
	return w.Subscribe(ctx)
}

func (b *Broker) Start() {
	for _, v := range b.qw {
		go v.Run()
	}
}

func (b *Broker) Stop() {
	for _, v := range b.qw {
		v.Stop()
	}
}

type QueueWorker struct {
	subs    []chan []byte
	queue   *SyncQueue
	hasSubs *sync.Cond
	// use this channel to stop worker
	quit chan struct{}
}

func NewQueueWorker(
	subs []chan []byte,
	queue *SyncQueue,
) *QueueWorker {
	return &QueueWorker{
		subs:    subs,
		queue:   queue,
		hasSubs: sync.NewCond(&sync.Mutex{}),
		quit:    make(chan struct{}),
	}
}

func (b *QueueWorker) Run() {
	for {
		msg, ok := b.queue.Get()
		if !ok {
			break
		}
		if !b.handleMessage(msg) {
			break
		}
		b.queue.Remove()
	}
}

func (b *QueueWorker) AddMessage(_ context.Context, msg []byte) error {
	return b.queue.Add(msg)
}

func (b *QueueWorker) Subscribe(_ context.Context) (<-chan []byte, error) {
	b.hasSubs.L.Lock()
	defer b.hasSubs.L.Unlock()

	if len(b.subs) == cap(b.subs) {
		return nil, ErrSubsMaxLimit
	}
	sb := make(chan []byte)
	b.subs = append(b.subs, sb)
	b.hasSubs.Signal()
	return sb, nil
}

func (b *QueueWorker) handleMessage(q []byte) bool {
	b.hasSubs.L.Lock()
	defer b.hasSubs.L.Unlock()

	msg := q
	for len(b.subs) == 0 {
		select {
		case <-b.quit:
			b.hasSubs.L.Unlock()
			return false
		default:
			b.hasSubs.Wait()
		}
	}
	for _, sub := range b.subs {
		sub <- msg
	}
	return true
}

func (b *QueueWorker) Stop() {
	b.queue.Close()
	b.hasSubs.Broadcast()
	for _, sub := range b.subs {
		close(sub)
	}
	close(b.quit)
}
