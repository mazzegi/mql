package mql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestExample(t *testing.T) {
	ExampleQueue()
}

func ExampleQueue() {
	store, err := NewSqliteStore(":memory:")
	PanicOnErr(err, "init sqlite store")
	defer store.Close()
	queue := NewQueue(store)

	var topic Topic = "topic_1"
	clientID := "client_1"

	//consume
	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msgs, err := queue.ReadContext(ctx, clientID, topic, 2, 50*time.Millisecond)
				PanicOnErr(err, "read")
				if len(msgs) == 0 {
					continue
				}
				for _, m := range msgs {
					fmt.Println(m.Index, string(m.Data))
				}
				err = queue.CommitContext(ctx, clientID, topic, msgs[len(msgs)-1].Index)
			}
		}
	}(ctx)

	//produce
	chunkSize := 5
	chunks := 10
	for i := 0; i < chunks; i++ {
		msgs := make([][]byte, chunkSize)
		for j := 0; j < chunkSize; j++ {
			msgs[j] = []byte(fmt.Sprintf("message_%03d", i*chunkSize+j))
		}
		queue.WriteContext(ctx, topic, msgs...)
		<-time.After(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()
}
