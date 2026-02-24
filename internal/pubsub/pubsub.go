package pubsub

import (
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/thirawat27/kvi/pkg/types"
)

// Hub manages all pub/sub channels
type Hub struct {
	channels map[string]*Channel
	mu       sync.RWMutex

	// Statistics
	publishCount  int64
	deliveryCount int64
}

// Channel represents a pub/sub channel
type Channel struct {
	Name      string
	Subs      map[string]*Subscriber
	Buffer    chan Message
	Retention int       // Number of messages to keep in history
	History   []Message // Message history
	mu        sync.RWMutex
}

// Subscriber represents a channel subscriber
type Subscriber struct {
	ID        string
	C         chan Message
	Patterns  []string // Patterns this subscriber is interested in
	Active    bool
	CreatedAt time.Time
}

// Message represents a pub/sub message
type Message struct {
	ID        string                 `json:"id"`
	Channel   string                 `json:"channel"`
	Data      interface{}            `json:"data"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

// NewHub creates a new pub/sub hub
func NewHub() *Hub {
	return &Hub{
		channels: make(map[string]*Channel),
	}
}

// CreateChannel creates a new channel
func (h *Hub) CreateChannel(name string, retention int) (*Channel, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, exists := h.channels[name]; exists {
		return nil, fmt.Errorf("channel %s already exists", name)
	}

	channel := &Channel{
		Name:      name,
		Subs:      make(map[string]*Subscriber),
		Buffer:    make(chan Message, 1000),
		Retention: retention,
		History:   make([]Message, 0),
	}

	h.channels[name] = channel

	// Start channel goroutine
	go channel.processBuffer()

	return channel, nil
}

// GetChannel retrieves a channel by name
func (h *Hub) GetChannel(name string) (*Channel, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	channel, exists := h.channels[name]
	if !exists {
		return nil, types.ErrChannelNotFound
	}

	return channel, nil
}

// DeleteChannel removes a channel
func (h *Hub) DeleteChannel(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	channel, exists := h.channels[name]
	if !exists {
		return types.ErrChannelNotFound
	}

	// Close all subscribers
	for _, sub := range channel.Subs {
		close(sub.C)
	}

	delete(h.channels, name)

	return nil
}

// Publish publishes a message to a channel
func (h *Hub) Publish(channel string, data interface{}) error {
	h.mu.RLock()
	ch, exists := h.channels[channel]
	h.mu.RUnlock()

	if !exists {
		return types.ErrChannelNotFound
	}

	msg := Message{
		ID:        generateMessageID(),
		Channel:   channel,
		Data:      data,
		Timestamp: time.Now(),
	}

	// Send to channel buffer
	select {
	case ch.Buffer <- msg:
		h.publishCount++
		return nil
	default:
		return fmt.Errorf("channel %s buffer full", channel)
	}
}

// Subscribe subscribes to a channel
func (h *Hub) Subscribe(channel string, subscriberID string) (*Subscriber, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch, exists := h.channels[channel]
	if !exists {
		return nil, types.ErrChannelNotFound
	}

	if _, exists := ch.Subs[subscriberID]; exists {
		return nil, fmt.Errorf("subscriber %s already exists", subscriberID)
	}

	sub := &Subscriber{
		ID:        subscriberID,
		C:         make(chan Message, 100),
		Active:    true,
		CreatedAt: time.Now(),
	}

	ch.Subs[subscriberID] = sub

	return sub, nil
}

// PSubscribe subscribes to channels matching a pattern (Redis-style)
func (h *Hub) PSubscribe(pattern string, subscriberID string) (*Subscriber, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	sub := &Subscriber{
		ID:        subscriberID,
		C:         make(chan Message, 100),
		Patterns:  []string{pattern},
		Active:    true,
		CreatedAt: time.Now(),
	}

	// Subscribe to all matching channels
	for name, ch := range h.channels {
		if matchPattern(pattern, name) {
			ch.Subs[subscriberID] = sub
		}
	}

	return sub, nil
}

// Unsubscribe removes a subscriber from a channel
func (h *Hub) Unsubscribe(channel string, subscriberID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ch, exists := h.channels[channel]
	if !exists {
		return types.ErrChannelNotFound
	}

	sub, exists := ch.Subs[subscriberID]
	if !exists {
		return fmt.Errorf("subscriber %s not found", subscriberID)
	}

	sub.Active = false
	close(sub.C)
	delete(ch.Subs, subscriberID)

	return nil
}

// UnsubscribeAll removes a subscriber from all channels
func (h *Hub) UnsubscribeAll(subscriberID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, ch := range h.channels {
		if sub, exists := ch.Subs[subscriberID]; exists {
			sub.Active = false
			close(sub.C)
			delete(ch.Subs, subscriberID)
		}
	}
}

// Stats returns hub statistics
func (h *Hub) Stats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var totalSubscribers int
	for _, ch := range h.channels {
		totalSubscribers += len(ch.Subs)
	}

	return HubStats{
		ChannelCount:    len(h.channels),
		SubscriberCount: totalSubscribers,
		PublishCount:    h.publishCount,
		DeliveryCount:   h.deliveryCount,
	}
}

// HubStats contains hub statistics
type HubStats struct {
	ChannelCount    int
	SubscriberCount int
	PublishCount    int64
	DeliveryCount   int64
}

// Channel methods

func (c *Channel) processBuffer() {
	for msg := range c.Buffer {
		c.mu.RLock()
		// Add to history
		c.History = append(c.History, msg)
		if c.Retention > 0 && len(c.History) > c.Retention {
			c.History = c.History[len(c.History)-c.Retention:]
		}

		// Broadcast to subscribers
		for _, sub := range c.Subs {
			if !sub.Active {
				continue
			}

			select {
			case sub.C <- msg:
				// Message delivered
			default:
				// Subscriber slow, drop message
			}
		}
		c.mu.RUnlock()
	}
}

// SubscriberCount returns the number of subscribers
func (c *Channel) SubscriberCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.Subs)
}

// GetHistory returns message history
func (c *Channel) GetHistory(limit int) []Message {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.History) {
		return c.History
	}

	return c.History[len(c.History)-limit:]
}

// Subscriber methods

// Receive receives a message from the subscriber
func (s *Subscriber) Receive() (Message, bool) {
	msg, ok := <-s.C
	return msg, ok
}

// ReceiveWithTimeout receives a message with timeout
func (s *Subscriber) ReceiveWithTimeout(timeout time.Duration) (Message, bool) {
	select {
	case msg, ok := <-s.C:
		return msg, ok
	case <-time.After(timeout):
		return Message{}, false
	}
}

// Helper functions

func generateMessageID() string {
	return fmt.Sprintf("msg_%d", time.Now().UnixNano())
}

func matchPattern(pattern, name string) bool {
	// Convert Redis-style pattern to regex
	// * matches any characters
	// ? matches single character
	regex := "^" + pattern + "$"
	regex = regexp.QuoteMeta(regex)
	regex = regexp.MustCompile(`\\\*`).ReplaceAllString(regex, ".*")
	regex = regexp.MustCompile(`\\\?`).ReplaceAllString(regex, ".")

	matched, _ := regexp.MatchString(regex, name)
	return matched
}
