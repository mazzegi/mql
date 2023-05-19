package mql

import (
	"context"
	"testing"
	"time"
)

func assertDurationInRange(t *testing.T, d time.Duration, lower time.Duration, upper time.Duration) {
	if d >= lower && d <= upper {
		return
	}
	t.Fatalf("duration %s not in range [%s, %s]", d, lower, upper)
}

func TestSignal(t *testing.T) {
	var topic1 Topic = "topic_x"
	//var topic2 Topic = "topic_2"

	sig := newSignal()
	emitAfter := func(t Topic, dur time.Duration) {
		time.AfterFunc(dur, func() { sig.emit(topic1) })
	}

	ctx := context.Background()
	t0 := time.Now()
	emitAfter(topic1, 20*time.Millisecond)
	ok := sig.waitContext(ctx, topic1, 50*time.Millisecond)
	if !ok {
		t.Fatalf("wait failed but shouldn't")
	}
	assertDurationInRange(t, time.Since(t0), 20*time.Millisecond, 30*time.Millisecond)

}
