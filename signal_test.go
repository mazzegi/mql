package mql

import (
	"context"
	"testing"
	"time"
)

func TestSignal(t *testing.T) {
	var topic1 Topic = "topic_1"
	var topic2 Topic = "topic_2"

	sig := newSignal()
	emitAfter := func(t Topic, dur time.Duration) {
		time.AfterFunc(dur, func() { sig.emit(t) })
	}

	ctx := context.Background()

	t0 := time.Now()
	emitAfter(topic1, 20*time.Millisecond)
	ok := sig.waitContext(ctx, topic1, 50*time.Millisecond)
	if !ok {
		t.Fatalf("wait failed but shouldn't")
	}
	AssertInRange(t, time.Since(t0), 20*time.Millisecond, 30*time.Millisecond)

	emitAfter(topic1, 50*time.Millisecond)
	ok = sig.waitContext(ctx, topic1, 20*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}
	ok = sig.waitContext(ctx, topic1, 50*time.Millisecond)
	if !ok {
		t.Fatalf("wait failed but shouldn't")
	}

	emitAfter(topic2, 20*time.Millisecond)
	ok = sig.waitContext(ctx, topic1, 50*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}

	dctx, dcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer dcancel()
	emitAfter(topic1, 50*time.Millisecond)
	ok = sig.waitContext(dctx, topic1, 20*time.Millisecond)
	if ok {
		t.Fatalf("wait didn't fail but should")
	}

}
