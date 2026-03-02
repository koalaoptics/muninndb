package cognitive

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestWorkerDroppedCounter verifies that dropped items are counted.
func TestWorkerDroppedCounter(t *testing.T) {
	// Create a worker with small buffer size
	bufSize := 2
	batchSize := 10
	maxWait := 100 * time.Millisecond

	w := NewWorker(bufSize, batchSize, maxWait, func(ctx context.Context, batch []int) error {
		return nil
	})

	// Fill the channel to capacity
	for i := 0; i < bufSize; i++ {
		ok := w.Submit(i)
		if !ok {
			t.Fatalf("expected Submit to succeed for item %d, but got dropped", i)
		}
	}

	// Submit one more item when full - should be dropped
	ok := w.Submit(999)
	if ok {
		t.Fatal("expected Submit to fail when channel is full, but it succeeded")
	}

	// Check stats
	stats := w.Stats()
	if stats.Dropped != 1 {
		t.Errorf("expected Dropped=1, got Dropped=%d", stats.Dropped)
	}
}

func TestWorkerBasic(t *testing.T) {
	var got []int
	w := NewWorker(10, 5, 50*time.Millisecond, func(ctx context.Context, batch []int) error {
		got = append(got, batch...)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Submit(1)
	w.Submit(2)
	w.Submit(3)
	time.Sleep(200 * time.Millisecond)

	stats := w.Stats()
	if stats.Processed < 3 {
		t.Fatalf("expected at least 3 processed, got %d", stats.Processed)
	}
}

func TestWorkerAdaptiveScaling(t *testing.T) {
	// Use a 2s base interval so that halving gives 1s, which is above the
	// 500ms minimum floor and well below the 2s base — proving tightening.
	// batchSize is larger than submit count so items never trigger an
	// immediate flush via the batch-full path; they accumulate until the
	// ticker fires, keeping pressure >75% at tick time.
	w := NewWorker(100, 200, 2*time.Second, func(ctx context.Context, batch []int) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	w.EnableAdaptiveScaling()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	// Submit enough items to fill >75% of the 100-item buffer.
	for i := 0; i < 80; i++ {
		w.Submit(i)
	}

	// Wait for the first tick (2s) plus processing headroom.
	time.Sleep(2500 * time.Millisecond)

	stats := w.Stats()
	if stats.EffectiveWait >= 2*time.Second {
		t.Fatalf("expected interval to tighten under pressure, got %v", stats.EffectiveWait)
	}
}

// TestWorker_NoGoroutineLeak verifies that cancelling the Run context causes
// the worker goroutine to exit cleanly, leaving no leaked goroutines.
func TestWorker_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	w := NewWorker(10, 5, 50*time.Millisecond, func(ctx context.Context, batch []int) error {
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx) //nolint:errcheck
	}()

	// Let the worker settle.
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop the worker.
	cancel()

	// Wait for the goroutine to exit.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker goroutine did not exit after context cancellation")
	}

	// Allow a brief moment for the runtime scheduler to reclaim the goroutine.
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	if after > before+2 {
		t.Errorf("goroutine leak: before=%d after=%d", before, after)
	}
}

func TestWorkerDormancy(t *testing.T) {
	w := NewWorker(10, 5, 50*time.Millisecond, func(ctx context.Context, batch []int) error {
		return nil
	})
	// Override thresholds for test speed.
	// dormantPoll will be set to dormant/5 = 40ms inside SetThresholds,
	// so the loop will re-evaluate state every 40ms while dormant.
	w.SetThresholds(100*time.Millisecond, 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.Submit(1)
	time.Sleep(10 * time.Millisecond)
	if w.Stats().State != WorkerStateActive {
		t.Fatal("expected Active after submission")
	}

	// Wait past idle threshold (100ms) but before dormant threshold (200ms).
	time.Sleep(150 * time.Millisecond)
	if w.Stats().State != WorkerStateIdle {
		t.Fatalf("expected Idle after idle threshold, got %v", w.Stats().State)
	}

	// Wait past dormant threshold (200ms total idle). The dormantPoll is 40ms
	// so the loop will fire and update state well within this window.
	time.Sleep(200 * time.Millisecond)
	if w.Stats().State != WorkerStateDormant {
		t.Fatalf("expected Dormant after dormant threshold, got %v", w.Stats().State)
	}

	// Wake it up.
	w.Submit(2)
	time.Sleep(10 * time.Millisecond)
	if w.Stats().State != WorkerStateActive {
		t.Fatalf("expected Active after wakeup, got %v", w.Stats().State)
	}
}
