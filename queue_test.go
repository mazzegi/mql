package mql

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	store, err := NewSqliteStore(":memory:")
	AssertNoErr(t, err, "init sqlite store")
	defer store.Close()
	queue := NewQueue(store)

	var topic Topic = "topic_1"
	clientID := "client_1"
	rawmsgs := [][]byte{}
	msgCount := 10
	for i := 0; i < msgCount; i++ {
		rawmsgs = append(rawmsgs, []byte(fmt.Sprintf("message_%03d", i)))
	}

	ctx := context.Background()
	err = queue.WriteContext(ctx, topic, rawmsgs...)
	AssertNoErr(t, err, "write")

	readCount := 5
	msgs, err := queue.ReadContext(ctx, clientID, topic, readCount, 0)
	AssertNoErr(t, err, "read")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i]), string(m.Data))
		AssertEqual(t, i, m.Index)
		AssertEqual(t, topic, m.Topic)
	}

	// same again (without commit)
	msgs, err = queue.ReadContext(ctx, clientID, topic, readCount, 0)
	AssertNoErr(t, err, "read")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i]), string(m.Data))
		AssertEqual(t, i, m.Index)
		AssertEqual(t, topic, m.Topic)
	}

	//commit
	err = queue.CommitContext(ctx, clientID, topic, msgs[len(msgs)-1].Index)
	AssertNoErr(t, err, "commit")

	msgs, err = queue.ReadContext(ctx, clientID, topic, readCount, 0)
	AssertNoErr(t, err, "read")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i+readCount]), string(m.Data))
		AssertEqual(t, i+readCount, m.Index)
		AssertEqual(t, topic, m.Topic)
	}
}

func TestQueueWait(t *testing.T) {
	store, err := NewSqliteStore(":memory:")
	AssertNoErr(t, err, "init sqlite store")
	defer store.Close()
	queue := NewQueue(store)

	var topic Topic = "topic_1"
	clientID := "client_1"
	rawmsgs := [][]byte{}
	msgCount := 5
	for i := 0; i < msgCount; i++ {
		rawmsgs = append(rawmsgs, []byte(fmt.Sprintf("message_%03d", i)))
	}

	ctx := context.Background()
	t0 := time.Now()
	time.AfterFunc(20*time.Millisecond, func() {
		err = queue.WriteContext(ctx, topic, rawmsgs...)
		AssertNoErr(t, err, "write")
	})
	readCount := 5
	msgs, err := queue.ReadContext(ctx, clientID, topic, readCount, 50*time.Millisecond)
	AssertNoErr(t, err, "read")
	AssertInRange(t, time.Since(t0), 20*time.Millisecond, 30*time.Millisecond)
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i]), string(m.Data))
		AssertEqual(t, i, m.Index)
		AssertEqual(t, topic, m.Topic)
	}
	err = queue.CommitContext(ctx, clientID, topic, msgs[len(msgs)-1].Index)
	AssertNoErr(t, err, "commit")

	t0 = time.Now()
	msgs, err = queue.ReadContext(ctx, clientID, topic, readCount, 50*time.Millisecond)
	AssertNoErr(t, err, "read")
	AssertInRange(t, time.Since(t0), 40*time.Millisecond, 60*time.Millisecond)
	AssertEqual(t, 0, len(msgs))

}
