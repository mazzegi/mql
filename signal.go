package mql

import (
	"context"
	"sync"
	"time"
)

type topicSignals map[chan struct{}]struct{}

func newSignals() *signals {
	return &signals{
		subs: make(map[Topic]topicSignals),
	}
}

type signals struct {
	sync.RWMutex
	subs map[Topic]topicSignals
}

func (s *signals) emit(topic Topic) {
	s.Lock()
	defer s.Unlock()
	for c := range s.subs[topic] {
		close(c)
	}
	delete(s.subs, topic)
}

func (s *signals) subscribe(topic Topic) chan struct{} {
	s.Lock()
	defer s.Unlock()
	c := make(chan struct{})
	if _, ok := s.subs[topic]; !ok {
		s.subs[topic] = make(topicSignals)
	}
	s.subs[topic][c] = struct{}{}
	return c
}

func (s *signals) unsubscribe(topic Topic, c chan struct{}) {
	s.Lock()
	defer s.Unlock()
	defer close(c)
	cs, ok := s.subs[topic]
	if !ok {
		return
	}
	delete(cs, c)
}

func (s *signals) waitContext(ctx context.Context, topic Topic, timeout time.Duration) bool {
	c := s.subscribe(topic)
	timer := time.NewTimer(timeout)
	select {
	case <-ctx.Done():
		s.unsubscribe(topic, c)
		return false
	case <-timer.C:
		s.unsubscribe(topic, c)
		return false
	case <-c:
		return true
	}
}
