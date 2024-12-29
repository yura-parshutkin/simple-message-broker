package mule

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	t.Run("Add and Take single item", func(t *testing.T) {
		q := NewQueue(10)
		node := []byte("test")
		err := q.Add(node)
		if err != nil {
			t.Errorf("Add failed: %v", err)
		}
		takenNode := q.Take()
		if string(takenNode) != string(node) {
			t.Errorf("Taken node is incorrect: got %s, want %s", string(takenNode), string(node))
		}
	})

	t.Run("Add exceeds max size", func(t *testing.T) {
		q := NewQueue(2)
		err := q.Add([]byte("one"))
		if err != nil {
			t.Errorf("Add failed: %v", err)
		}
		err = q.Add([]byte("two"))
		if err != nil {
			t.Errorf("Add failed: %v", err)
		}
		err = q.Add([]byte("three"))
		if !errors.Is(err, ErrQueueMaxLimit) {
			t.Errorf("Expected ErrQueueMaxLimit, got: %v", err)
		}
	})

	t.Run("Get waits for item", func(t *testing.T) {
		q := NewQueue(10)
		var gotNode []byte
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			gotNode = q.Get()
		}()

		time.Sleep(100 * time.Millisecond) // Give the goroutine time to start waiting

		node := []byte("test")
		err := q.Add(node)
		if err != nil {
			t.Errorf("Add failed: %v", err)
		}

		wg.Wait() // Wait for the Get goroutine to finish
		if string(gotNode) != string(node) {
			t.Errorf("Got incorrect node: got %s, want %s", string(gotNode), string(node))
		}
	})

	t.Run("Multiple Add and Take", func(t *testing.T) {
		q := NewQueue(5)
		nodes := [][]byte{
			[]byte("one"),
			[]byte("two"),
			[]byte("three"),
		}

		for _, node := range nodes {
			err := q.Add(node)
			if err != nil {
				t.Errorf("Add failed: %v", err)
			}
		}

		for _, expectedNode := range nodes {
			takenNode := q.Take()
			if string(takenNode) != string(expectedNode) {
				t.Errorf("Taken node is incorrect: got %s, want %s", string(takenNode), string(expectedNode))
			}
		}
	})

	t.Run("Concurrent Add and Take", func(t *testing.T) {
		q := NewQueue(10)
		numItems := 5
		var wg sync.WaitGroup

		for i := 0; i < numItems; i++ {
			wg.Add(1)
			go func(val int) {
				defer wg.Done()
				err := q.Add([]byte(fmt.Sprintf("item%d", val)))
				if err != nil {
					t.Errorf("Add failed: %v", err)
				}
			}(i)
		}

		for i := 0; i < numItems; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.Take()
			}()
		}

		wg.Wait()
		if len(q.nodes) != 0 {
			t.Errorf("Queue should be empty after concurrent operations, but has %d elements", len(q.nodes))
		}
	})
}
