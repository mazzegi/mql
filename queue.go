package mql

import (
	"context"
	"fmt"
	"time"
)

type Topic string

type Message struct {
	Topic Topic
	Index int
	Data  []byte
}

type Store interface {
	AppendContext(ctx context.Context, topic Topic, msgs ...[]byte) error
	CommitContext(ctx context.Context, clientID string, topic Topic, idx int) error
	FetchNextContext(ctx context.Context, clientID string, topic Topic, limit int) ([]Message, error)
}

func NewQueue(store Store) *Queue {
	return &Queue{
		store: store,
		sig:   newSignal(),
	}
}

type Queue struct {
	store Store
	sig   *signal
}

func (q *Queue) WriteContext(ctx context.Context, topic Topic, msgs ...[]byte) error {
	err := q.store.AppendContext(ctx, topic, msgs...)
	if err != nil {
		return fmt.Errorf("store.append: %w", err)
	}
	q.sig.emit(topic)
	return nil
}

func (q *Queue) ReadContext(ctx context.Context, clientID string, topic Topic, limit int) ([]Message, error) {
	msgs, err := q.store.FetchNextContext(ctx, clientID, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("store.fetchnext: %w", err)
	}
	return msgs, nil
}

func (q *Queue) ReadWaitContext(ctx context.Context, clientID string, topic Topic, limit int, wait time.Duration) ([]Message, error) {
	msgs, err := q.store.FetchNextContext(ctx, clientID, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("store.fetchnext: %w", err)
	}
	if len(msgs) > 0 || wait == 0 {
		return msgs, nil
	}
	if !q.sig.waitContext(ctx, topic, wait) {
		return msgs, nil
	}
	// read again without timeout
	return q.ReadContext(ctx, clientID, topic, limit)
}

func (q *Queue) CommitContext(ctx context.Context, clientID string, topic Topic, idx int) error {
	err := q.store.CommitContext(ctx, clientID, topic, idx)
	if err != nil {
		return fmt.Errorf("store.commit: %w", err)
	}
	return nil
}
