package main

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand"
	"strings"

	"github.com/scrypster/muninndb/internal/cognitive"
	"github.com/scrypster/muninndb/internal/engine/activation"
	"github.com/scrypster/muninndb/internal/engine/trigger"
	"github.com/scrypster/muninndb/internal/index/fts"
	hnswpkg "github.com/scrypster/muninndb/internal/index/hnsw"
	embedpkg "github.com/scrypster/muninndb/internal/plugin/embed"
	"github.com/scrypster/muninndb/internal/storage"
)

// hashEmbedder produces deterministic 384-dim unit vectors from text.
// Used as fallback when the local MiniLM model is not available.
type hashEmbedder struct{}

func (e *hashEmbedder) Embed(_ context.Context, texts []string) ([]float32, error) {
	const dims = 384
	vec := make([]float64, dims)
	for _, text := range texts {
		for _, word := range strings.Fields(strings.ToLower(text)) {
			h := fnv.New64a()
			h.Write([]byte(word))
			rng := rand.New(rand.NewSource(int64(h.Sum64()))) //nolint:gosec
			for i := range vec {
				vec[i] += rng.NormFloat64()
			}
		}
	}
	var norm float64
	for _, v := range vec {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	out := make([]float32, dims)
	if norm > 0 {
		for i, v := range vec {
			out[i] = float32(v / norm)
		}
	}
	return out, nil
}

func (e *hashEmbedder) Tokenize(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

// bibleEmbedder wraps EmbedService to implement activation.Embedder.
// Multiple context strings are joined before embedding so HNSW gets a single
// dim-sized vector (not a concatenated flat array).
type bibleEmbedder struct {
	svc *embedpkg.EmbedService
}

func (e *bibleEmbedder) Embed(ctx context.Context, texts []string) ([]float32, error) {
	combined := strings.Join(texts, ". ")
	return e.svc.Embed(ctx, []string{combined})
}

func (e *bibleEmbedder) Tokenize(text string) []string {
	return strings.Fields(strings.ToLower(text))
}

// hnswActAdapter adapts hnsw.Registry to activation.HNSWIndex.
type hnswActAdapter struct{ r *hnswpkg.Registry }

func (a *hnswActAdapter) Search(ctx context.Context, ws [8]byte, vec []float32, topK int) ([]activation.ScoredID, error) {
	results, err := a.r.Search(ctx, ws, vec, topK)
	if err != nil {
		return nil, err
	}
	out := make([]activation.ScoredID, len(results))
	for i, r := range results {
		out[i] = activation.ScoredID{ID: storage.ULID(r.ID), Score: r.Score}
	}
	return out, nil
}

// ftsAdapter adapts fts.Index to activation.FTSIndex.
type ftsAdapter struct{ idx *fts.Index }

func (a *ftsAdapter) Search(ctx context.Context, ws [8]byte, query string, topK int) ([]activation.ScoredID, error) {
	results, err := a.idx.Search(ctx, ws, query, topK)
	if err != nil {
		return nil, err
	}
	out := make([]activation.ScoredID, len(results))
	for i, r := range results {
		out[i] = activation.ScoredID{ID: storage.ULID(r.ID), Score: r.Score}
	}
	return out, nil
}

// ftsTrigAdapter adapts fts.Index to trigger.FTSIndex.
type ftsTrigAdapter struct{ idx *fts.Index }

func (a *ftsTrigAdapter) Search(ctx context.Context, ws [8]byte, query string, topK int) ([]trigger.ScoredID, error) {
	results, err := a.idx.Search(ctx, ws, query, topK)
	if err != nil {
		return nil, err
	}
	out := make([]trigger.ScoredID, len(results))
	for i, r := range results {
		out[i] = trigger.ScoredID{ID: storage.ULID(r.ID), Score: r.Score}
	}
	return out, nil
}

// bibleHebbianAdapter adapts PebbleStore to the HebbianStore interface.
type bibleHebbianAdapter struct{ store *storage.PebbleStore }

func (a *bibleHebbianAdapter) GetAssocWeight(ctx context.Context, ws [8]byte, src, dst [16]byte) (float32, error) {
	return a.store.GetAssocWeight(ctx, ws, storage.ULID(src), storage.ULID(dst))
}
func (a *bibleHebbianAdapter) UpdateAssocWeight(ctx context.Context, ws [8]byte, src, dst [16]byte, w float32) error {
	return a.store.UpdateAssocWeight(ctx, ws, storage.ULID(src), storage.ULID(dst), w)
}
func (a *bibleHebbianAdapter) DecayAssocWeights(ctx context.Context, ws [8]byte, factor float64, min float32) (int, error) {
	return a.store.DecayAssocWeights(ctx, ws, factor, min)
}
func (a *bibleHebbianAdapter) UpdateAssocWeightBatch(ctx context.Context, updates []cognitive.AssocWeightUpdate) error {
	storageUpdates := make([]storage.AssocWeightUpdate, len(updates))
	for i, u := range updates {
		storageUpdates[i] = storage.AssocWeightUpdate{
			WS:     u.WS,
			Src:    storage.ULID(u.Src),
			Dst:    storage.ULID(u.Dst),
			Weight: u.Weight,
		}
	}
	return a.store.UpdateAssocWeightBatch(ctx, storageUpdates)
}

// bibleDecayAdapter adapts PebbleStore to the DecayStore interface.
type bibleDecayAdapter struct{ store *storage.PebbleStore }

func (a *bibleDecayAdapter) GetMetadataBatch(ctx context.Context, ws [8]byte, ids [][16]byte) ([]cognitive.DecayMeta, error) {
	ulidIDs := make([]storage.ULID, len(ids))
	for i, id := range ids {
		ulidIDs[i] = storage.ULID(id)
	}
	metas, err := a.store.GetMetadata(ctx, ws, ulidIDs)
	if err != nil {
		return nil, err
	}
	result := make([]cognitive.DecayMeta, len(metas))
	for i, meta := range metas {
		if meta != nil {
			result[i] = cognitive.DecayMeta{
				ID:          [16]byte(meta.ID),
				LastAccess:  meta.LastAccess,
				AccessCount: meta.AccessCount,
				Stability:   meta.Stability,
				Relevance:   meta.Relevance,
			}
		}
	}
	return result, nil
}
func (a *bibleDecayAdapter) UpdateRelevance(ctx context.Context, ws [8]byte, id [16]byte, relevance, stability float32) error {
	return a.store.UpdateRelevance(ctx, ws, storage.ULID(id), relevance, stability)
}

// bibleConfidenceAdapter adapts PebbleStore to the ConfidenceStore interface.
type bibleConfidenceAdapter struct{ store *storage.PebbleStore }

func (a *bibleConfidenceAdapter) GetConfidence(ctx context.Context, ws [8]byte, id [16]byte) (float32, error) {
	return a.store.GetConfidence(ctx, ws, storage.ULID(id))
}
func (a *bibleConfidenceAdapter) UpdateConfidence(ctx context.Context, ws [8]byte, id [16]byte, c float32) error {
	return a.store.UpdateConfidence(ctx, ws, storage.ULID(id), c)
}

// bibleContradictAdapter adapts PebbleStore to the ContradictionStore interface.
type bibleContradictAdapter struct{ store *storage.PebbleStore }

func (a *bibleContradictAdapter) FlagContradiction(ctx context.Context, ws [8]byte, engramA, engramB [16]byte) error {
	return a.store.FlagContradiction(ctx, ws, storage.ULID(engramA), storage.ULID(engramB))
}
