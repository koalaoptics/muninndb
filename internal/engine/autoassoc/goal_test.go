package autoassoc

import (
	"context"
	"testing"
	"time"

	"github.com/scrypster/muninndb/internal/index/hnsw"
	"github.com/scrypster/muninndb/internal/storage"
)

type fakeGoalStore struct {
	written int
}

func (f *fakeGoalStore) WriteAssociation(_ context.Context, _ [8]byte, _, _ storage.ULID, _ *storage.Association) error {
	f.written++
	return nil
}

type fakeGoalHNSW struct {
	results []hnsw.ScoredID
}

func (f *fakeGoalHNSW) Search(_ context.Context, _ [8]byte, _ []float32, _ int) ([]hnsw.ScoredID, error) {
	return f.results, nil
}

func TestGoalLinkWorker_EnqueuesAndProcesses(t *testing.T) {
	store := &fakeGoalStore{}
	hnswIdx := &fakeGoalHNSW{
		results: []hnsw.ScoredID{
			{ID: [16]byte{1}, Score: 0.8},
			{ID: [16]byte{2}, Score: 0.5}, // below threshold — should be skipped
		},
	}
	w := NewGoalLinkWorker(store, hnswIdx)
	var ws [8]byte
	var id [16]byte
	id[0] = 99
	w.EnqueueGoalJob(GoalJob{WS: ws, ID: id, Embedding: []float32{0.1, 0.2}})
	w.Stop()
	// Should write 1 association (score 0.8 >= 0.6, score 0.5 < 0.6)
	if store.written != 1 {
		t.Fatalf("want 1 WriteAssociation call, got %d", store.written)
	}
}

func TestGoalLinkWorker_DropsSelfLink(t *testing.T) {
	var id [16]byte
	id[0] = 42
	store := &fakeGoalStore{}
	hnswIdx := &fakeGoalHNSW{
		results: []hnsw.ScoredID{{ID: id, Score: 0.9}}, // same ID as job
	}
	w := NewGoalLinkWorker(store, hnswIdx)
	w.EnqueueGoalJob(GoalJob{WS: [8]byte{}, ID: id, Embedding: []float32{0.1}})
	w.Stop()
	if store.written != 0 {
		t.Fatalf("self-link should be skipped, got %d writes", store.written)
	}
}

func TestGoalLinkWorker_Stop(t *testing.T) {
	w := NewGoalLinkWorker(&fakeGoalStore{}, &fakeGoalHNSW{})
	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return in time")
	}
}
