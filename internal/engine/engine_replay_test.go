package engine

import (
	"context"
	"testing"

	"github.com/scrypster/muninndb/internal/plugin"
	"github.com/scrypster/muninndb/internal/storage"
	"github.com/scrypster/muninndb/internal/transport/mbp"
)

// mockEnrichPlugin implements plugin.EnrichPlugin for testing.
type mockEnrichPlugin struct {
	enrichFn func(ctx context.Context, eng *storage.Engram) (*plugin.EnrichmentResult, error)
	calls    int
}

func (m *mockEnrichPlugin) Name() string  { return "mock-enrich" }
func (m *mockEnrichPlugin) Tier() plugin.PluginTier { return plugin.TierEnrich }
func (m *mockEnrichPlugin) Init(_ context.Context, _ plugin.PluginConfig) error { return nil }
func (m *mockEnrichPlugin) Close() error  { return nil }

func (m *mockEnrichPlugin) Enrich(ctx context.Context, eng *storage.Engram) (*plugin.EnrichmentResult, error) {
	m.calls++
	if m.enrichFn != nil {
		return m.enrichFn(ctx, eng)
	}
	return &plugin.EnrichmentResult{
		Summary:   "mock summary",
		KeyPoints: []string{"point1"},
	}, nil
}

// TestReplayEnrichment_DryRunNoModification verifies that dry_run=true returns
// a count of what would be processed without actually writing enrichment data.
func TestReplayEnrichment_DryRunNoModification(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Write two engrams (no enrich plugin set).
	for i := 0; i < 2; i++ {
		_, err := eng.Write(ctx, &mbp.WriteRequest{
			Vault:   "default",
			Content: "content for engram",
			Concept: "test concept",
		})
		if err != nil {
			t.Fatalf("Write: %v", err)
		}
	}

	// Dry run — no enrichPlugin set, should still succeed because we only scan.
	result, err := eng.ReplayEnrichment(ctx, "default", nil, 50, true)
	if err != nil {
		t.Fatalf("ReplayEnrichment(dry_run=true): %v", err)
	}
	if !result.DryRun {
		t.Error("expected DryRun=true in result")
	}
	// Both engrams have no digest flags set, so all should be counted as needing enrichment.
	if result.Processed < 2 {
		t.Errorf("expected at least 2 engrams to need enrichment, got %d", result.Processed)
	}
	// Verify no enrichment actually ran (no enrichPlugin was set).
	// Checking the dry_run field is sufficient: engine would error on real run without plugin.
}

// TestReplayEnrichment_SkipsAlreadyEnriched verifies that engrams with all
// requested digest flags already set are skipped (counted in Skipped, not Processed).
func TestReplayEnrichment_SkipsAlreadyEnriched(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Write one engram.
	resp, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   "default",
		Content: "already enriched content",
		Concept: "fully enriched",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Manually set all per-stage digest flags on the engram.
	id, err := storage.ParseULID(resp.ID)
	if err != nil {
		t.Fatalf("ParseULID: %v", err)
	}
	allFlags := plugin.DigestEntities | plugin.DigestRelationships | plugin.DigestClassified | plugin.DigestSummarized
	for _, flag := range []uint8{
		plugin.DigestEntities,
		plugin.DigestRelationships,
		plugin.DigestClassified,
		plugin.DigestSummarized,
	} {
		if err := eng.store.SetDigestFlag(ctx, id, flag); err != nil {
			t.Fatalf("SetDigestFlag(0x%02x): %v", flag, err)
		}
	}
	_ = allFlags

	mock := &mockEnrichPlugin{}
	eng.SetEnrichPlugin(mock)

	result, err := eng.ReplayEnrichment(ctx, "default", nil, 50, false)
	if err != nil {
		t.Fatalf("ReplayEnrichment: %v", err)
	}

	if result.Skipped < 1 {
		t.Errorf("expected at least 1 skipped (fully enriched), got %d", result.Skipped)
	}
	if mock.calls > 0 {
		t.Errorf("expected 0 enrich calls for fully-enriched engram, got %d", mock.calls)
	}
}

// TestReplayEnrichment_NoPipelineReturnsError verifies that if no enrich plugin
// is configured and dry_run=false, an appropriate error is returned.
func TestReplayEnrichment_NoPipelineReturnsError(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	ctx := context.Background()

	// Write one engram.
	_, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   "default",
		Content: "needs enrichment",
		Concept: "no plugin",
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// No enrichPlugin is set on the engine.
	_, err = eng.ReplayEnrichment(ctx, "default", nil, 50, false)
	if err == nil {
		t.Fatal("expected error when no enrich plugin is configured, got nil")
	}
	if err.Error() != "enrichment pipeline not configured: no enrich plugin available" {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestReplayEnrichment_DryRunEmptyVault verifies that a vault with no engrams
// returns zero counts without error.
func TestReplayEnrichment_DryRunEmptyVault(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	result, err := eng.ReplayEnrichment(context.Background(), "empty-vault", nil, 50, true)
	if err != nil {
		t.Fatalf("ReplayEnrichment on empty vault: %v", err)
	}
	if result.Processed != 0 || result.Skipped != 0 {
		t.Errorf("expected 0/0 for empty vault, got processed=%d skipped=%d",
			result.Processed, result.Skipped)
	}
}

// TestReplayEnrichment_InvalidStageName verifies that unknown stage names
// return an error immediately.
func TestReplayEnrichment_InvalidStageName(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	_, err := eng.ReplayEnrichment(context.Background(), "default", []string{"bogus_stage"}, 50, true)
	if err == nil {
		t.Fatal("expected error for unknown stage name, got nil")
	}
}

// TestReplayEnrichment_StagesRunReflectsRequest verifies that StagesRun in the
// result matches the requested stages.
func TestReplayEnrichment_StagesRunReflectsRequest(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	requested := []string{"summary", "classification"}
	result, err := eng.ReplayEnrichment(context.Background(), "default", requested, 50, true)
	if err != nil {
		t.Fatalf("ReplayEnrichment: %v", err)
	}

	if len(result.StagesRun) != len(requested) {
		t.Fatalf("StagesRun length: got %d, want %d", len(result.StagesRun), len(requested))
	}
	for i, stage := range requested {
		if result.StagesRun[i] != stage {
			t.Errorf("StagesRun[%d] = %q, want %q", i, result.StagesRun[i], stage)
		}
	}
}
