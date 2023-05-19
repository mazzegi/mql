package mql

import (
	"context"
	"fmt"
	"testing"
)

func TestSqliteStore(t *testing.T) {
	store, err := NewSqliteStore(":memory:")
	AssertNoErr(t, err, "init sqlite store")
	defer store.Close()

	var topic Topic = "topic_1"
	clientID := "client_1"
	rawmsgs := [][]byte{}
	msgCount := 10
	for i := 0; i < msgCount; i++ {
		rawmsgs = append(rawmsgs, []byte(fmt.Sprintf("message_%03d", i)))
	}

	ctx := context.Background()
	err = store.AppendContext(ctx, topic, rawmsgs...)
	AssertNoErr(t, err, "append-context")

	readCount := 5
	msgs, err := store.FetchNextContext(ctx, clientID, topic, readCount)
	AssertNoErr(t, err, "fetch-next")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i]), string(m.Data))
		AssertEqual(t, i, m.Index)
		AssertEqual(t, topic, m.Topic)
	}

	// same again (without commit)
	msgs, err = store.FetchNextContext(ctx, clientID, topic, readCount)
	AssertNoErr(t, err, "fetch-next")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i]), string(m.Data))
		AssertEqual(t, i, m.Index)
		AssertEqual(t, topic, m.Topic)
	}

	//commit
	err = store.CommitContext(ctx, clientID, topic, msgs[len(msgs)-1].Index)
	AssertNoErr(t, err, "commit")

	msgs, err = store.FetchNextContext(ctx, clientID, topic, readCount)
	AssertNoErr(t, err, "fetch-next")
	for i := 0; i < readCount; i++ {
		m := msgs[i]
		AssertEqual(t, string(rawmsgs[i+readCount]), string(m.Data))
		AssertEqual(t, i+readCount, m.Index)
		AssertEqual(t, topic, m.Topic)
	}
}
