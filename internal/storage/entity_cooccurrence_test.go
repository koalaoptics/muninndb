package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIncrementEntityCoOccurrence_PairCanonical verifies that writing (B,A) and
// (A,B) for the same entity pair produces a single entry with count=2.
func TestIncrementEntityCoOccurrence_PairCanonical(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ws := store.VaultPrefix("cooccurrence-canonical")

	// First increment with (B,A) order.
	err := store.IncrementEntityCoOccurrence(ctx, ws, "Redis", "PostgreSQL")
	require.NoError(t, err)

	// Second increment with (A,B) order — should hit the same key.
	err = store.IncrementEntityCoOccurrence(ctx, ws, "PostgreSQL", "Redis")
	require.NoError(t, err)

	// Scan: should see exactly one pair with count=2.
	var pairs []struct {
		a, b  string
		count int
	}
	err = store.ScanEntityClusters(ctx, ws, 1, func(nameA, nameB string, count int) error {
		pairs = append(pairs, struct {
			a, b  string
			count int
		}{nameA, nameB, count})
		return nil
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1, "canonical pair order must deduplicate to one entry")
	assert.Equal(t, 2, pairs[0].count, "count should be 2 after two increments")
}

// TestScanEntityClusters_MinCount verifies that only pairs with count >= minCount are returned.
func TestScanEntityClusters_MinCount(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	ws := store.VaultPrefix("cooccurrence-mincount")

	// Pair A,B — increment once.
	require.NoError(t, store.IncrementEntityCoOccurrence(ctx, ws, "Go", "PostgreSQL"))

	// Pair C,D — increment three times.
	for i := 0; i < 3; i++ {
		require.NoError(t, store.IncrementEntityCoOccurrence(ctx, ws, "Redis", "Kafka"))
	}

	// With minCount=2, only the Redis+Kafka pair should appear.
	var pairs []struct{ a, b string }
	err := store.ScanEntityClusters(ctx, ws, 2, func(nameA, nameB string, count int) error {
		pairs = append(pairs, struct{ a, b string }{nameA, nameB})
		return nil
	})
	require.NoError(t, err)
	require.Len(t, pairs, 1, "only pairs with count >= minCount should be returned")

	// With minCount=1, both pairs should appear.
	var allPairs []struct{ a, b string }
	err = store.ScanEntityClusters(ctx, ws, 1, func(nameA, nameB string, count int) error {
		allPairs = append(allPairs, struct{ a, b string }{nameA, nameB})
		return nil
	})
	require.NoError(t, err)
	assert.Len(t, allPairs, 2, "both pairs should appear with minCount=1")
}

// TestScanEntityClusters_MultipleVaults verifies that co-occurrences from other
// vaults are not returned when scanning a specific vault.
func TestScanEntityClusters_MultipleVaults(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	wsA := store.VaultPrefix("vault-alpha")
	wsB := store.VaultPrefix("vault-beta")

	// Write to vault A.
	require.NoError(t, store.IncrementEntityCoOccurrence(ctx, wsA, "Rust", "Go"))

	// Write to vault B.
	require.NoError(t, store.IncrementEntityCoOccurrence(ctx, wsB, "Python", "Ruby"))
	require.NoError(t, store.IncrementEntityCoOccurrence(ctx, wsB, "Python", "Ruby"))

	// Scan vault A — should only see Rust+Go.
	var pairsA []string
	err := store.ScanEntityClusters(ctx, wsA, 1, func(nameA, nameB string, count int) error {
		pairsA = append(pairsA, nameA+"+"+nameB)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, pairsA, 1, "vault A scan must not return vault B pairs")

	// Scan vault B — should only see Python+Ruby.
	var pairsB []string
	err = store.ScanEntityClusters(ctx, wsB, 1, func(nameA, nameB string, count int) error {
		pairsB = append(pairsB, nameA+"+"+nameB)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, pairsB, 1, "vault B scan must not return vault A pairs")
}
