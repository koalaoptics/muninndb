package main

import (
	"context"
	"fmt"

	"github.com/scrypster/muninndb/internal/cognitive"
	"github.com/scrypster/muninndb/internal/engine"
	"github.com/scrypster/muninndb/internal/engine/activation"
	"github.com/scrypster/muninndb/internal/engine/trigger"
	"github.com/scrypster/muninndb/internal/index/fts"
	hnswpkg "github.com/scrypster/muninndb/internal/index/hnsw"
	"github.com/scrypster/muninndb/internal/plugin"
	embedpkg "github.com/scrypster/muninndb/internal/plugin/embed"
	"github.com/scrypster/muninndb/internal/storage"
	"github.com/scrypster/muninndb/internal/transport/mbp"
)

// evalEngine bundles all the engine components needed for Bible eval.
type evalEngine struct {
	eng           *engine.Engine
	store         *storage.PebbleStore
	hnswReg       *hnswpkg.Registry
	embedder      activation.Embedder
	ws            [8]byte
	cancel        context.CancelFunc
	hebbianWorker *cognitive.HebbianWorker
}

// newEvalEngine initialises the full MuninnDB engine stack for evaluation.
// It follows cmd/eval/main.go exactly: OpenPebble → NewPebbleStore → fts.New →
// hnswpkg.NewRegistry → embedpkg.NewEmbedService → activation.New → engine.NewEngine.
func newEvalEngine(ctx context.Context, dataDir string) (*evalEngine, error) {
	db, err := storage.OpenPebble(dataDir, storage.DefaultOptions())
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	store := storage.NewPebbleStore(db, 100_000)
	ftsIdx := fts.New(db)
	hnswReg := hnswpkg.NewRegistry(db)

	// Use bundled all-MiniLM-L6-v2 embedder (FTS-only fallback if unavailable)
	svc, initErr := embedpkg.NewEmbedService("local://all-MiniLM-L6-v2")
	if initErr != nil {
		// Fall back to no HNSW — FTS only
		svc = nil
	}
	var embedder activation.Embedder
	var hnswIdx activation.HNSWIndex
	if svc != nil {
		if initErr = svc.Init(ctx, plugin.PluginConfig{DataDir: dataDir}); initErr != nil {
			// Model assets not present — run FTS-only
			svc = nil
		}
	}
	if svc != nil {
		embedder = &bibleEmbedder{svc: svc}
		hnswIdx = &hnswActAdapter{r: hnswReg}
	} else {
		embedder = &hashEmbedder{}
		hnswIdx = nil
	}

	actEngine := activation.New(store, &ftsAdapter{ftsIdx}, hnswIdx, embedder)
	trigSystem := trigger.New(store, &ftsTrigAdapter{ftsIdx}, nil, embedder)

	hebbianWorker := cognitive.NewHebbianWorker(&bibleHebbianAdapter{store})
	decayWorker := cognitive.NewDecayWorker(&bibleDecayAdapter{store})
	contradictWorker := cognitive.NewContradictWorker(&bibleContradictAdapter{store})
	confidenceWorker := cognitive.NewConfidenceWorker(&bibleConfidenceAdapter{store})

	workerCtx, workerCancel := context.WithCancel(context.Background())
	go decayWorker.Worker.Run(workerCtx)
	go contradictWorker.Worker.Run(workerCtx)
	go confidenceWorker.Worker.Run(workerCtx)

	eng := engine.NewEngine(
		store, nil, ftsIdx, actEngine, trigSystem,
		hebbianWorker, decayWorker,
		contradictWorker.Worker, confidenceWorker.Worker,
		embedder, hnswReg,
	)

	ws := store.ResolveVaultPrefix("bible")

	return &evalEngine{
		eng:           eng,
		store:         store,
		hnswReg:       hnswReg,
		embedder:      embedder,
		ws:            ws,
		cancel:        workerCancel,
		hebbianWorker: hebbianWorker,
	}, nil
}

// close stops all background workers and releases storage.
// Order mirrors cmd/eval/main.go: cancel context → stop Hebbian → stop engine → close store.
func (ee *evalEngine) close() {
	ee.cancel()
	if ee.hebbianWorker != nil {
		ee.hebbianWorker.Stop()
	}
	ee.eng.Stop()
	ee.store.Close()
}

// writeVerse writes one verse WriteRequest to the engine and indexes it in HNSW.
func (ee *evalEngine) writeVerse(ctx context.Context, req mbp.WriteRequest) (storage.ULID, error) {
	req.Vault = "bible"
	resp, err := ee.eng.Write(ctx, &req)
	if err != nil {
		return storage.ULID{}, fmt.Errorf("write verse %q: %w", req.Concept, err)
	}

	id, parseErr := storage.ParseULID(resp.ID)
	if parseErr != nil {
		return storage.ULID{}, fmt.Errorf("parse ULID %q: %w", resp.ID, parseErr)
	}

	// Embed and insert into HNSW if index is available.
	if ee.hnswReg != nil {
		text := req.Concept
		if req.Content != "" {
			text += ". " + req.Content
		}
		vec, embedErr := ee.embedder.Embed(ctx, []string{text})
		if embedErr == nil {
			_ = ee.hnswReg.Insert(ctx, ee.ws, [16]byte(id), vec)
		}
	}

	return id, nil
}

// activate queries the engine with the given context strings and returns the top-10 results.
func (ee *evalEngine) activate(ctx context.Context, contextStrs []string) ([]mbp.ActivationItem, error) {
	resp, err := ee.eng.Activate(ctx, &mbp.ActivateRequest{
		Context:    contextStrs,
		MaxResults: 10,
		Vault:      "bible",
		BriefMode:  "off",
	})
	if err != nil {
		return nil, err
	}
	return resp.Activations, nil
}
