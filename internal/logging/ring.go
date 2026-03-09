package logging

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// LogEntry holds a single log record for ring buffer storage.
type LogEntry struct {
	Level string
	Time  time.Time
	Msg   string
	Attrs map[string]string
}

// RingBuffer is a thread-safe bounded circular buffer of LogEntry.
// When full, new entries overwrite the oldest.
// onAdd (if non-nil) is called after each Add, outside the mutex.
type RingBuffer struct {
	mu      sync.Mutex
	entries []LogEntry
	cap     int
	head    int // index of next write position
	count   int // number of valid entries (≤ cap)
	onAdd   func(LogEntry)
}

// NewRingBuffer creates a ring buffer with the given capacity.
// onAdd is called after every Add, outside the lock. May be nil.
func NewRingBuffer(cap int, onAdd func(LogEntry)) *RingBuffer {
	return &RingBuffer{
		entries: make([]LogEntry, cap),
		cap:     cap,
		onAdd:   onAdd,
	}
}

// SetOnAdd sets (or replaces) the onAdd callback. Thread-safe.
func (rb *RingBuffer) SetOnAdd(fn func(LogEntry)) {
	rb.mu.Lock()
	rb.onAdd = fn
	rb.mu.Unlock()
}

// Add appends an entry to the ring, overwriting the oldest if full.
// onAdd is called after the lock is released.
func (rb *RingBuffer) Add(e LogEntry) {
	rb.mu.Lock()
	rb.entries[rb.head] = e
	rb.head = (rb.head + 1) % rb.cap
	if rb.count < rb.cap {
		rb.count++
	}
	onAdd := rb.onAdd
	rb.mu.Unlock()

	if onAdd != nil {
		onAdd(e)
	}
}

// Clear removes all entries from the ring buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	rb.head = 0
	rb.count = 0
	rb.mu.Unlock()
}

// Snapshot returns all current entries in insertion order (oldest first).
func (rb *RingBuffer) Snapshot() []LogEntry {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return nil
	}

	out := make([]LogEntry, rb.count)
	// If not full, entries start at index 0.
	// If full, oldest entry is at head (the next write position).
	start := 0
	if rb.count == rb.cap {
		start = rb.head
	}
	for i := 0; i < rb.count; i++ {
		out[i] = rb.entries[(start+i)%rb.cap]
	}
	return out
}

// RingHandler implements slog.Handler, writing to a base handler AND a RingBuffer.
// WithAttrs and WithGroup return new instances — the receiver is never mutated.
type RingHandler struct {
	base   slog.Handler
	ring   *RingBuffer
	attrs  []slog.Attr
	groups []string
}

// NewRingHandler wraps base and appends each record to ring.
func NewRingHandler(base slog.Handler, ring *RingBuffer) *RingHandler {
	return &RingHandler{base: base, ring: ring}
}

// Enabled delegates to the base handler.
func (h *RingHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.base.Enabled(ctx, l)
}

// Handle writes to the base handler and appends a LogEntry to the ring.
func (h *RingHandler) Handle(ctx context.Context, r slog.Record) error {
	attrs := make(map[string]string)
	for _, a := range h.attrs {
		attrs[h.groupKey(a.Key)] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[h.groupKey(a.Key)] = a.Value.String()
		return true
	})
	h.ring.Add(LogEntry{
		Level: r.Level.String(),
		Time:  r.Time,
		Msg:   r.Message,
		Attrs: attrs,
	})
	return h.base.Handle(ctx, r)
}

// WithAttrs returns a new RingHandler with additional attrs. Does NOT mutate h.
func (h *RingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &RingHandler{
		base:   h.base.WithAttrs(attrs),
		ring:   h.ring,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

// WithGroup returns a new RingHandler with the group name appended. Does NOT mutate h.
func (h *RingHandler) WithGroup(name string) slog.Handler {
	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name
	return &RingHandler{
		base:   h.base.WithGroup(name),
		ring:   h.ring,
		attrs:  h.attrs,
		groups: newGroups,
	}
}

// groupKey prefixes key with the current group path (e.g., "request.method").
func (h *RingHandler) groupKey(key string) string {
	if len(h.groups) == 0 {
		return key
	}
	return strings.Join(h.groups, ".") + "." + key
}
