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

func (b *Broker) Run() {
	for _, v := range b.qw {
		go v.Run()
	}
}

type QueueWorker struct {
	subs  []chan []byte
	queue *SyncQueue
	cond  *sync.Cond
}

func NewQueueWorker(
	subs []chan []byte,
	queue *SyncQueue,
) *QueueWorker {
	return &QueueWorker{
		subs:  subs,
		queue: queue,
		cond:  sync.NewCond(&sync.Mutex{}),
	}
}

func (b *QueueWorker) Run() {
	for {
		msg := b.queue.Get()
		b.readMessage(msg)
		b.queue.Take()
	}
}

func (b *QueueWorker) AddMessage(_ context.Context, msg []byte) error {
	return b.queue.Add(msg)
}

func (b *QueueWorker) Subscribe(_ context.Context) (<-chan []byte, error) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	if len(b.subs) == cap(b.subs) {
		return nil, ErrSubsMaxLimit
	}
	sb := make(chan []byte)
	b.subs = append(b.subs, sb)
	b.cond.Signal()
	return sb, nil
}

func (b *QueueWorker) readMessage(q []byte) {
	b.cond.L.Lock()
	defer b.cond.L.Unlock()

	msg := q
	for len(b.subs) == 0 {
		b.cond.Wait()
	}
	for _, sub := range b.subs {
		sub <- msg
	}
}
