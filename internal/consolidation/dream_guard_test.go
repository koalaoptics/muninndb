package consolidation

import (
	"context"
	"testing"

	"github.com/scrypster/muninndb/internal/storage"
)

// TestDream_SmallVault_SkipsDedup verifies that Phase 2 dedup is skipped when
// the vault has fewer engrams than MinDedupVaultSize. This guards against the
// normalization anchor flip documented in issue #311: removing even a small
// duplicate cluster from a <20 engram vault can shift the per-query
// normalization landscape and flip top-1 recall results.
func TestDream_SmallVault_SkipsDedup(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "small-vault-guard"
	wsPrefix := store.ResolveVaultPrefix(vault)

	// Write a vault that's below MinDedupVaultSize (default 20) but contains
	// a clear duplicate pair (cosine >= 0.95) that WOULD be merged if dedup ran.
	// We use 9 engrams: below the 20-engram threshold.
	dup := []float32{1.0, 0.0, 0.0, 0.0}
	dupClose := []float32{0.97, 0.24310, 0.0, 0.0} // cosine ≈ 0.97

	var dupAID, dupBID storage.ULID
	for i := 0; i < 9; i++ {
		embed := []float32{0, 0, float32(i + 1), 0}
		if i == 0 {
			embed = dup
		} else if i == 1 {
			embed = dupClose
		}
		eng := &storage.Engram{
			Concept: "engram", Content: "content", Confidence: 0.8, Relevance: 0.7, Stability: 20,
			Embedding: embed,
		}
		id := writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, eng)
		if i == 0 {
			dupAID = id
		} else if i == 1 {
			dupBID = id
		}
	}

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock)
	// MinDedupVaultSize defaults to 20; vault has 9 engrams — guard must fire.

	report, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(report.Reports))
	}

	// Phase 2 must not have run — MergedEngrams must be zero.
	if report.Reports[0].MergedEngrams != 0 {
		t.Errorf("small vault: MergedEngrams = %d, want 0 (guard should have skipped dedup)",
			report.Reports[0].MergedEngrams)
	}

	// Both engrams (including the duplicate pair) must still be active.
	for _, id := range []storage.ULID{dupAID, dupBID} {
		eng, err := store.GetEngram(ctx, wsPrefix, id)
		if err != nil {
			t.Fatalf("GetEngram %v: %v", id, err)
		}
		if eng.State == storage.StateArchived {
			t.Errorf("engram %v was archived in a small vault — guard should have prevented this", id)
		}
	}
}

// TestDream_SufficientVault_RunsDedup verifies that Phase 2 dedup DOES run when
// the vault meets MinDedupVaultSize, and correctly archives the lower-quality
// member of a duplicate pair while preserving the representative.
func TestDream_SufficientVault_RunsDedup(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "sufficient-vault-dedup"
	wsPrefix := store.ResolveVaultPrefix(vault)

	// Build a vault at exactly MinDedupVaultSize (20 engrams) with one clear
	// duplicate pair. All other engrams are orthogonal (unique class).
	dup := []float32{1.0, 0.0, 0.0, 0.0}
	dupClose := []float32{0.97, 0.24310, 0.0, 0.0} // cosine ≈ 0.97

	representativeEng := &storage.Engram{
		Concept: "dup-representative", Content: "France's capital is Paris.",
		Confidence: 0.9, Relevance: 0.85, Stability: 30, Embedding: dup,
	}
	memberEng := &storage.Engram{
		Concept: "dup-member", Content: "Paris is the capital of France.",
		Confidence: 0.5, Relevance: 0.5, Stability: 20, Embedding: dupClose,
	}

	repID := writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, representativeEng)
	memID := writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, memberEng)

	// Pad to 20 engrams with orthogonal unique engrams.
	for i := 0; i < 18; i++ {
		embed := make([]float32, 20)
		embed[i+2] = 1.0 // orthogonal unit vector in 20-dim space
		writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
			Concept: "unique", Content: "unique content", Confidence: 0.8, Relevance: 0.7,
			Stability: 25, Embedding: embed,
		})
	}

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock)

	report, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Reports) != 1 {
		t.Fatalf("expected 1 report, got %d", len(report.Reports))
	}

	// Dedup must have run and merged exactly 1 pair.
	if report.Reports[0].MergedEngrams != 1 {
		t.Errorf("MergedEngrams = %d, want 1", report.Reports[0].MergedEngrams)
	}

	// Representative (higher confidence*relevance = 0.9*0.85 = 0.765) must be active.
	rep, err := store.GetEngram(ctx, wsPrefix, repID)
	if err != nil {
		t.Fatal(err)
	}
	if rep.State == storage.StateArchived {
		t.Error("representative engram was archived — wrong member elected")
	}

	// Member (lower confidence*relevance = 0.5*0.5 = 0.25) must be archived.
	mem, err := store.GetEngram(ctx, wsPrefix, memID)
	if err != nil {
		t.Fatal(err)
	}
	if mem.State != storage.StateArchived {
		t.Errorf("member engram state = %v, want StateArchived", mem.State)
	}
}

// TestDream_Dedup_PreservesUniqueEngrams verifies that engrams with low cross-similarity
// (the "unique" class) are never archived by Phase 2, regardless of vault size.
func TestDream_Dedup_PreservesUniqueEngrams(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "unique-preservation"
	wsPrefix := store.ResolveVaultPrefix(vault)

	// Build a 20-engram vault using the synthetic vault helper.
	// The helper writes 9 labeled engrams; pad to 20 with neutrals.
	entries := buildSyntheticVault(t, ctx, store, db, wsPrefix)
	for i := 0; i < 11; i++ {
		embed := make([]float32, 20)
		embed[i+4] = 1.0
		writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
			Concept: "pad", Content: "padding engram", Confidence: 0.6, Relevance: 0.5,
			Stability: 20, Embedding: embed,
		})
	}

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock)

	_, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}

	// All unique-class engrams must still be active.
	for _, class := range []syntheticClass{classUniqueA, classUniqueB, classLowAccessUnique} {
		for _, id := range findByClass(entries, class) {
			eng, err := store.GetEngram(ctx, wsPrefix, id)
			if err != nil {
				t.Fatalf("GetEngram %v: %v", id, err)
			}
			if eng.State == storage.StateArchived {
				t.Errorf("unique engram (class %d, id %v) was archived — information loss", class, id)
			}
		}
	}
}

// TestDream_NearDuplicates_NotAutoMerged verifies that engrams in the 0.85–0.95
// cosine similarity band are NOT automatically archived at the default 0.95 threshold.
// These require human or LLM review (Phase 2b, future PR).
func TestDream_NearDuplicates_NotAutoMerged(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "near-dup-review-band"
	wsPrefix := store.ResolveVaultPrefix(vault)

	entries := buildSyntheticVault(t, ctx, store, db, wsPrefix)
	for i := 0; i < 11; i++ {
		embed := make([]float32, 20)
		embed[i+4] = 1.0
		writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
			Concept: "pad", Content: "padding", Confidence: 0.6, Relevance: 0.5,
			Stability: 20, Embedding: embed,
		})
	}

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock) // default threshold 0.95

	_, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}

	// Both near-duplicate engrams must remain active — cosine 0.90 < threshold 0.95.
	for _, class := range []syntheticClass{classNearDuplicateA, classNearDuplicateB} {
		for _, id := range findByClass(entries, class) {
			eng, err := store.GetEngram(ctx, wsPrefix, id)
			if err != nil {
				t.Fatalf("GetEngram %v: %v", id, err)
			}
			if eng.State == storage.StateArchived {
				t.Errorf("near-duplicate engram %v was auto-archived; cosine=0.90 is below the 0.95 threshold and requires review", id)
			}
		}
	}
}

// TestDream_LegalVault_ZeroWrites verifies that legal-scoped vaults receive zero
// mutations across all dream phases. The vault name "legal/contracts" must match
// the isLegalVault() prefix convention.
func TestDream_LegalVault_ZeroWrites(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "legal/contracts"
	wsPrefix := store.ResolveVaultPrefix(vault)

	// Write a clear duplicate pair into the legal vault — if dedup ran, one would be archived.
	dup  := []float32{1.0, 0.0, 0.0, 0.0}
	dupClose := []float32{0.97, 0.24310, 0.0, 0.0}
	id1 := writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "clause-a", Content: "Party A agrees to pay Party B.", Confidence: 0.9,
		Relevance: 0.9, Stability: 40, Embedding: dup,
	})
	id2 := writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "clause-a-copy", Content: "Party A shall pay Party B.", Confidence: 0.85,
		Relevance: 0.85, Stability: 35, Embedding: dupClose,
	})

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock)

	report, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}

	// Vault must appear in Skipped list.
	if len(report.Skipped) != 1 || report.Skipped[0] != vault {
		t.Errorf("expected %q in Skipped, got %v", vault, report.Skipped)
	}
	if len(report.Reports) != 1 || report.Reports[0].MergedEngrams != 0 {
		t.Errorf("legal vault: MergedEngrams = %d, want 0", report.Reports[0].MergedEngrams)
	}

	// Both engrams must be untouched.
	for _, id := range []storage.ULID{id1, id2} {
		eng, err := store.GetEngram(ctx, wsPrefix, id)
		if err != nil {
			t.Fatalf("GetEngram %v: %v", id, err)
		}
		if eng.State != storage.StateActive {
			t.Errorf("legal vault engram %v state = %v, want StateActive (legal vaults must not be touched)", id, eng.State)
		}
	}
}

// TestDream_LegalAdjacent_IsProcessed verifies that vaults whose names contain
// "legal" as a substring (e.g. "paralegal-notes") are NOT classified as legal
// vaults and ARE processed normally by the dream engine.
// This guards against an overly broad legal-vault check that would silence
// legitimate vaults via substring match.
func TestDream_LegalAdjacent_IsProcessed(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "paralegal-notes"
	wsPrefix := store.ResolveVaultPrefix(vault)

	// Write 20 engrams including a clear duplicate pair. If the vault is incorrectly
	// treated as legal, dedup will be skipped and MergedEngrams will be 0.
	dup     := []float32{1.0, 0.0, 0.0, 0.0}
	dupClose := []float32{0.97, 0.24310, 0.0, 0.0}

	writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "note-a", Content: "The hearing is on Monday.", Confidence: 0.9,
		Relevance: 0.85, Stability: 30, Embedding: dup,
	})
	writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "note-a-dup", Content: "Monday is when the hearing takes place.", Confidence: 0.5,
		Relevance: 0.5, Stability: 20, Embedding: dupClose,
	})
	for i := 0; i < 18; i++ {
		embed := make([]float32, 20)
		embed[i+2] = 1.0
		writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
			Concept: "unique", Content: "unique note", Confidence: 0.7, Relevance: 0.6,
			Stability: 22, Embedding: embed,
		})
	}

	mock := &mockEngineInterface{store: store}
	w := NewWorker(mock)

	report, err := w.DreamOnce(ctx, DreamOpts{Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}

	// "paralegal-notes" must NOT be in the Skipped list.
	for _, skipped := range report.Skipped {
		if skipped == vault {
			t.Errorf("vault %q was incorrectly classified as legal and skipped", vault)
		}
	}

	// Dedup must have run and merged the duplicate pair.
	if len(report.Reports) != 1 || report.Reports[0].MergedEngrams == 0 {
		t.Errorf("paralegal-notes vault: MergedEngrams = %d, want > 0 (vault should be processed normally)",
			report.Reports[0].MergedEngrams)
	}
}

// TestDream_MinDedupVaultSize_Configurable verifies that MinDedupVaultSize is
// respected when set explicitly on the Worker. A vault of 15 engrams with a
// MinDedupVaultSize of 10 must run dedup; the same vault with MinDedupVaultSize
// of 20 must skip it.
func TestDream_MinDedupVaultSize_Configurable(t *testing.T) {
	t.Parallel()
	store, db, cleanup := testStoreWithDB(t)
	defer cleanup()
	ctx := context.Background()

	const vault = "configurable-guard"
	wsPrefix := store.ResolveVaultPrefix(vault)

	dup     := []float32{1.0, 0.0, 0.0, 0.0}
	dupClose := []float32{0.97, 0.24310, 0.0, 0.0}

	writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "d-a", Content: "content a", Confidence: 0.9, Relevance: 0.8,
		Stability: 25, Embedding: dup,
	})
	writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
		Concept: "d-b", Content: "content b", Confidence: 0.5, Relevance: 0.5,
		Stability: 20, Embedding: dupClose,
	})
	for i := 0; i < 13; i++ {
		embed := make([]float32, 15)
		embed[i+2] = 1.0
		writeEngramWithEmbedding(t, ctx, store, db, wsPrefix, &storage.Engram{
			Concept: "u", Content: "u", Confidence: 0.7, Relevance: 0.6,
			Stability: 20, Embedding: embed,
		})
	}
	// vault now has 15 engrams

	mock := &mockEngineInterface{store: store}

	// With MinDedupVaultSize=10: 15 >= 10, dedup runs.
	w1 := NewWorker(mock)
	w1.MinDedupVaultSize = 10
	report1, err := w1.DreamOnce(ctx, DreamOpts{DryRun: true, Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}
	// In DryRun the dedup logic runs (clusters are found) but no mutations occur.
	// DedupClusters > 0 confirms Phase 2 was reached and found the pair.
	if report1.Reports[0].DedupClusters == 0 {
		t.Errorf("MinDedupVaultSize=10, vault=15: expected DedupClusters > 0 (dedup should have run)")
	}

	// With MinDedupVaultSize=20: 15 < 20, dedup skipped.
	w2 := NewWorker(mock)
	w2.MinDedupVaultSize = 20
	report2, err := w2.DreamOnce(ctx, DreamOpts{DryRun: true, Force: true, Scope: vault})
	if err != nil {
		t.Fatal(err)
	}
	if report2.Reports[0].DedupClusters != 0 || report2.Reports[0].MergedEngrams != 0 {
		t.Errorf("MinDedupVaultSize=20, vault=15: DedupClusters=%d MergedEngrams=%d, want both 0 (guard should skip)",
			report2.Reports[0].DedupClusters, report2.Reports[0].MergedEngrams)
	}
}
