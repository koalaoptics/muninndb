package logging_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/scrypster/muninndb/internal/logging"
)

func TestRingBuffer_AddAndSnapshot(t *testing.T) {
	rb := logging.NewRingBuffer(3, nil)
	e1 := logging.LogEntry{Level: "INFO", Msg: "first", Time: time.Now()}
	e2 := logging.LogEntry{Level: "WARN", Msg: "second", Time: time.Now()}
	e3 := logging.LogEntry{Level: "ERROR", Msg: "third", Time: time.Now()}

	rb.Add(e1)
	rb.Add(e2)
	rb.Add(e3)

	snap := rb.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(snap))
	}
	if snap[0].Msg != "first" || snap[1].Msg != "second" || snap[2].Msg != "third" {
		t.Errorf("unexpected snapshot order: %+v", snap)
	}
}

func TestRingBuffer_CapacityWraps(t *testing.T) {
	rb := logging.NewRingBuffer(2, nil)
	rb.Add(logging.LogEntry{Msg: "a"})
	rb.Add(logging.LogEntry{Msg: "b"})
	rb.Add(logging.LogEntry{Msg: "c"}) // wraps, evicts "a"

	snap := rb.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(snap))
	}
	if snap[0].Msg != "b" || snap[1].Msg != "c" {
		t.Errorf("expected [b c], got %+v", snap)
	}
}

func TestRingBuffer_OnAddCallback(t *testing.T) {
	var called []string
	rb := logging.NewRingBuffer(10, func(e logging.LogEntry) {
		called = append(called, e.Msg)
	})
	rb.Add(logging.LogEntry{Msg: "x"})
	rb.Add(logging.LogEntry{Msg: "y"})

	if len(called) != 2 || called[0] != "x" || called[1] != "y" {
		t.Errorf("onAdd not called correctly: %v", called)
	}
}

func TestRingBuffer_NilOnAddIsSafe(t *testing.T) {
	rb := logging.NewRingBuffer(5, nil)
	// Must not panic
	rb.Add(logging.LogEntry{Msg: "safe"})
}

func TestRingBuffer_ConcurrentAdd(t *testing.T) {
	rb := logging.NewRingBuffer(1000, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				rb.Add(logging.LogEntry{Msg: "concurrent"})
			}
		}()
	}
	wg.Wait()
	snap := rb.Snapshot()
	if len(snap) != 1000 {
		t.Errorf("expected 1000 entries (ring full), got %d", len(snap))
	}
}

func TestRingBuffer_SetOnAdd(t *testing.T) {
	var called int
	rb := logging.NewRingBuffer(10, nil)
	rb.Add(logging.LogEntry{Msg: "before"}) // onAdd is nil, no call

	rb.SetOnAdd(func(e logging.LogEntry) { called++ })
	rb.Add(logging.LogEntry{Msg: "after"})

	if called != 1 {
		t.Errorf("expected 1 call after SetOnAdd, got %d", called)
	}
}

func TestRingHandler_LogsAppearInRing(t *testing.T) {
	rb := logging.NewRingBuffer(100, nil)
	base := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := logging.NewRingHandler(base, rb)
	logger := slog.New(h)

	logger.Info("hello world", "key", "value")

	snap := rb.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	if snap[0].Msg != "hello world" {
		t.Errorf("unexpected msg: %q", snap[0].Msg)
	}
	if snap[0].Attrs["key"] != "value" {
		t.Errorf("expected attr key=value, got %+v", snap[0].Attrs)
	}
	if snap[0].Level != "INFO" {
		t.Errorf("expected level INFO, got %q", snap[0].Level)
	}
}

func TestRingHandler_WithAttrsIsImmutable(t *testing.T) {
	rb := logging.NewRingBuffer(100, nil)
	base := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := logging.NewRingHandler(base, rb)

	h2 := h.WithAttrs([]slog.Attr{slog.String("service", "muninn")})
	logger2 := slog.New(h2)
	logger2.Info("with attrs")

	snap := rb.Snapshot()
	if snap[0].Attrs["service"] != "muninn" {
		t.Errorf("expected service attr in child handler, got %+v", snap[0].Attrs)
	}

	// Original handler should not see the attr
	logger := slog.New(h)
	logger.Info("without attrs")

	snap2 := rb.Snapshot()
	last := snap2[len(snap2)-1]
	if _, ok := last.Attrs["service"]; ok {
		t.Errorf("original handler should not have service attr, attrs: %+v", last.Attrs)
	}
}

func TestRingHandler_EnabledDelegatesToBase(t *testing.T) {
	rb := logging.NewRingBuffer(100, nil)
	base := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn})
	h := logging.NewRingHandler(base, rb)

	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("DEBUG should not be enabled when base is Warn+")
	}
	if !h.Enabled(context.Background(), slog.LevelError) {
		t.Error("ERROR should be enabled when base is Warn+")
	}
}

func TestRingBuffer_Clear(t *testing.T) {
	rb := logging.NewRingBuffer(5, nil)
	rb.Add(logging.LogEntry{Msg: "a"})
	rb.Add(logging.LogEntry{Msg: "b"})
	rb.Add(logging.LogEntry{Msg: "c"})

	rb.Clear()

	snap := rb.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("expected 0 entries after Clear, got %d", len(snap))
	}

	// Verify the buffer is usable after clearing.
	rb.Add(logging.LogEntry{Msg: "d"})
	snap = rb.Snapshot()
	if len(snap) != 1 || snap[0].Msg != "d" {
		t.Errorf("expected [d] after Clear + Add, got %+v", snap)
	}
}
