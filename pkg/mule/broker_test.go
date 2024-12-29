package mule

import (
	"context"
	"errors"
	"testing"
)

func TestBrokerHappyPass(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      10,
		SubsSize:  10,
	}})
	br.Run()
	sb, err := br.Subscribe(ctx, "test")
	if err != nil {
		t.Errorf("error subscribe not expected %v", err)
	}
	errAdd := br.AddMessage(ctx, "test", []byte("hello world"))
	if errAdd != nil {
		t.Errorf("error add message not expected %v", errAdd)
	}
	out := <-sb
	if string(out) != "hello world" {
		t.Errorf("message %s expected, actual %s", "hello world", string(out))
	}
}

func TestBrokerWitSeveralSubs(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      10,
		SubsSize:  10,
	}})
	br.Run()
	subs := make([]<-chan []byte, 0, 3)
	for i := 0; i < 3; i++ {
		sub, err := br.Subscribe(ctx, "test")
		if err != nil {
			t.Errorf("error subscribe not expected %v", err)
		}
		subs = append(subs, sub)
	}
	errAdd := br.AddMessage(ctx, "test", []byte("hello world"))
	if errAdd != nil {
		t.Errorf("error add message not expected %v", errAdd)
	}
	for _, sub := range subs {
		out := <-sub
		if string(out) != "hello world" {
			t.Errorf("message %s expected, actual %s", "hello world", string(out))
		}
	}
}

func TestBrokerQueueMaxLimitReached(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      1,
		SubsSize:  10,
	}})
	br.Run()
	var err error
	err = br.AddMessage(ctx, "test", []byte("hello world"))
	if err != nil {
		t.Errorf("error add message not expected %v", err)
	}
	err = br.AddMessage(ctx, "test", []byte("hello world"))
	if !errors.Is(err, ErrQueueMaxLimit) {
		t.Errorf("expected ErrQueueMaxLimit, actual %v", err)
	}
}

func TestBrokerSubsMaxLimitReached(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      1,
		SubsSize:  1,
	}})
	br.Run()
	var err error
	_, err = br.Subscribe(ctx, "test")
	if err != nil {
		t.Errorf("error subscribe not expected %v", err)
	}
	_, err = br.Subscribe(ctx, "test")
	if !errors.Is(err, ErrSubsMaxLimit) {
		t.Errorf("expected ErrSubsMaxLimit, actual %v", err)
	}
}

func TestBrokerQueueNotFound(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      10,
		SubsSize:  10,
	}})
	br.Run()
	var err error
	_, err = br.Subscribe(ctx, "test1")
	if !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, actual %v", err)
	}
	err = br.AddMessage(ctx, "test3", []byte("hello world"))
	if !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("expected ErrQueueNotFound, actual %v", err)
	}
}

func TestBrokerSendToNewSub(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{{
		QueueName: "test",
		Size:      10,
		SubsSize:  10,
	}})
	br.Run()

	sb1, _ := br.Subscribe(ctx, "test")
	_ = br.AddMessage(ctx, "test", []byte("hello world 1"))
	if string(<-sb1) != "hello world 1" {
		t.Errorf("message %s expected", "hello world 1")
	}

	sb2, _ := br.Subscribe(ctx, "test")
	_ = br.AddMessage(ctx, "test", []byte("hello world 2"))
	if string(<-sb1) != "hello world 2" {
		t.Errorf("message %s expected", "hello world 2")
	}
	if string(<-sb2) != "hello world 2" {
		t.Errorf("message %s expected", "hello world 2")
	}
}

func TestBrokerSendDifferenceQueue(t *testing.T) {
	ctx := context.Background()
	br := NewBroker(Config{
		{
			QueueName: "test1",
			Size:      10,
			SubsSize:  10,
		},
		{
			QueueName: "test2",
			Size:      10,
			SubsSize:  10,
		},
	})
	br.Run()

	sb1, _ := br.Subscribe(ctx, "test1")
	sb2, _ := br.Subscribe(ctx, "test2")

	_ = br.AddMessage(ctx, "test1", []byte("hello test1"))
	_ = br.AddMessage(ctx, "test2", []byte("hello test2"))

	if string(<-sb1) != "hello test1" {
		t.Errorf("message %s expected", "hello test1")
	}
	if string(<-sb2) != "hello test2" {
		t.Errorf("message %s expected", "hello test1")
	}
}
