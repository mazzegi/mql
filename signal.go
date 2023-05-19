package mql

import (
	"context"
	"sync"
	"time"
)

type topicSignal map[chan struct{}]struct{}

func newSignal() *signal {
	return &signal{
		subs: make(map[Topic]topicSignal),
	}
}

type signal struct {
	sync.RWMutex
	subs map[Topic]topicSignal
}

func (s *signal) emit(topic Topic) {
	s.Lock()
	defer s.Unlock()
	for c := range s.subs[topic] {
		close(c)
	}
	delete(s.subs, topic)
}

func (s *signal) subscribe(topic Topic) chan struct{} {
	s.Lock()
	defer s.Unlock()
	c := make(chan struct{})
	if _, ok := s.subs[topic]; !ok {
		s.subs[topic] = make(topicSignal)
	}
	s.subs[topic][c] = struct{}{}
	return c
}

func (s *signal) unsubscribe(topic Topic, c chan struct{}) {
	s.Lock()
	defer s.Unlock()
	cs, ok := s.subs[topic]
	if !ok {
		return
	}
	delete(cs, c)
}

func (s *signal) waitContext(ctx context.Context, topic Topic, timeout time.Duration) bool {
	c := s.subscribe(topic)
	defer s.unsubscribe(topic, c)
	timer := time.NewTimer(timeout)
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	case <-c:
		return true
	}
}
