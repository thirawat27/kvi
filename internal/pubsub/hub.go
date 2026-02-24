package pubsub

import (
	"regexp"
	"strings"
	"sync"
)

type Message struct {
	Channel string
	Payload string
}

type Subscriber struct {
	ID       string
	C        chan Message
	Patterns []string
	Active   bool
	mu       sync.Mutex
}

func NewSubscriber(id string) *Subscriber {
	return &Subscriber{
		ID:     id,
		C:      make(chan Message, 100),
		Active: true,
	}
}

func (s *Subscriber) Receive() (Message, bool) {
	msg, ok := <-s.C
	return msg, ok
}

type Channel struct {
	Name      string
	Subs      map[string]*Subscriber
	History   []Message
	Retention int
	mu        sync.RWMutex
}

type Hub struct {
	channels map[string]*Channel
	mu       sync.RWMutex
}

func NewHub() *Hub {
	return &Hub{
		channels: make(map[string]*Channel),
	}
}

func (h *Hub) getOrCreateChannel(name string) *Channel {
	h.mu.Lock()
	defer h.mu.Unlock()

	if ch, exists := h.channels[name]; exists {
		return ch
	}

	ch := &Channel{
		Name:      name,
		Subs:      make(map[string]*Subscriber),
		Retention: 100, // keep last 100 messages
	}
	h.channels[name] = ch
	return ch
}

func (h *Hub) Publish(channelName, payload string) int {
	ch := h.getOrCreateChannel(channelName)
	msg := Message{Channel: channelName, Payload: payload}

	ch.mu.Lock()
	ch.History = append(ch.History, msg)
	if len(ch.History) > ch.Retention {
		ch.History = ch.History[1:]
	}

	count := 0
	for _, sub := range ch.Subs {
		sub.mu.Lock()
		if sub.Active {
			select {
			case sub.C <- msg:
				count++
			default:
				// buffer full, skip or handle
			}
		}
		sub.mu.Unlock()
	}
	ch.mu.Unlock()

	// Handle Pattern subscriptions
	h.mu.RLock()
	defer h.mu.RUnlock()
	for name, patternCh := range h.channels {
		if strings.Contains(name, "*") { // simplified pattern check
			regexPattern := "^" + strings.ReplaceAll(name, "*", ".*") + "$"
			if matched, _ := regexp.MatchString(regexPattern, channelName); matched {
				patternCh.mu.RLock()
				for _, sub := range patternCh.Subs {
					sub.mu.Lock()
					if sub.Active {
						select {
						case sub.C <- msg:
							count++
						default:
						}
					}
					sub.mu.Unlock()
				}
				patternCh.mu.RUnlock()
			}
		}
	}

	return count
}

func (h *Hub) Subscribe(channelName, subscriberID string) *Subscriber {
	ch := h.getOrCreateChannel(channelName)

	ch.mu.Lock()
	defer ch.mu.Unlock()

	sub := NewSubscriber(subscriberID)
	ch.Subs[subscriberID] = sub
	return sub
}

func (h *Hub) PSubscribe(pattern, subscriberID string) *Subscriber {
	// PSubscribe creates a channel for the pattern
	return h.Subscribe(pattern, subscriberID)
}

func (h *Hub) Unsubscribe(channelName, subscriberID string) {
	h.mu.RLock()
	ch, exists := h.channels[channelName]
	h.mu.RUnlock()

	if !exists {
		return
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()

	if sub, exists := ch.Subs[subscriberID]; exists {
		sub.mu.Lock()
		sub.Active = false
		close(sub.C)
		sub.mu.Unlock()
		delete(ch.Subs, subscriberID)
	}
}
