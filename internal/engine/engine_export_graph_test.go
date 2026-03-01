package engine

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/scrypster/muninndb/internal/storage"
	"github.com/scrypster/muninndb/internal/transport/mbp"
	"github.com/stretchr/testify/require"
)

// writeEntityRelationship writes a memory with two entities and a relationship
// so that the 0x21 relationship index is populated.
func writeEntityRelationship(t *testing.T, eng *Engine, vault, fromName, fromType, toName, toType, relType string) {
	t.Helper()
	ctx := context.Background()
	_, err := eng.Write(ctx, &mbp.WriteRequest{
		Vault:   vault,
		Content: fromName + " " + relType + " " + toName,
		Concept: fromName + " → " + toName,
		Entities: []mbp.InlineEntity{
			{Name: fromName, Type: fromType},
			{Name: toName, Type: toType},
		},
		Relationships: []mbp.InlineRelationship{},
	})
	require.NoError(t, err)

	// Also upsert the relationship record directly so the 0x21 index is populated.
	ws := eng.store.ResolveVaultPrefix(vault)
	// Create a placeholder ULID for the relationship entry.
	id := storage.ULID{}
	err = eng.store.UpsertRelationshipRecord(ctx, ws, id, storage.RelationshipRecord{
		FromEntity: fromName,
		ToEntity:   toName,
		RelType:    relType,
		Weight:     0.8,
		Source:     "test",
	})
	require.NoError(t, err)

	// Upsert entity records so GetEntityRecord works.
	err = eng.store.UpsertEntityRecord(ctx, storage.EntityRecord{
		Name: fromName, Type: fromType, Confidence: 1.0,
	}, "test")
	require.NoError(t, err)
	err = eng.store.UpsertEntityRecord(ctx, storage.EntityRecord{
		Name: toName, Type: toType, Confidence: 1.0,
	}, "test")
	require.NoError(t, err)
}

func TestExportGraph_JSONLDFormat(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	writeEntityRelationship(t, eng, "default", "PostgreSQL", "database", "Redis", "cache", "manages")

	g, err := eng.ExportGraph(context.Background(), "default", true)
	require.NoError(t, err)
	require.NotNil(t, g)

	// Should have 2 nodes (PostgreSQL, Redis) and 1 edge.
	require.Len(t, g.Edges, 1, "expected 1 edge")
	require.Len(t, g.Nodes, 2, "expected 2 nodes")

	jsonLD, err := FormatGraphJSONLD(g)
	require.NoError(t, err)
	require.NotEmpty(t, jsonLD)

	// Parse as JSON and verify @context and @graph.
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(jsonLD), &doc))

	ctx, ok := doc["@context"].(map[string]any)
	require.True(t, ok, "expected @context to be a map")
	require.Equal(t, "https://schema.org/", ctx["@vocab"])
	require.Equal(t, "https://muninndb.io/ontology#", ctx["muninn"])

	graph, ok := doc["@graph"].([]any)
	require.True(t, ok, "expected @graph to be an array")
	// 2 entity nodes + 1 relationship node
	require.GreaterOrEqual(t, len(graph), 2, "expected at least 2 entries in @graph")

	// Find a relationship entry.
	foundRel := false
	for _, item := range graph {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if m["@type"] == "muninn:Relationship" {
			foundRel = true
			require.Equal(t, "manages", m["muninn:relType"])
		}
	}
	require.True(t, foundRel, "expected at least one muninn:Relationship in @graph")
}

func TestExportGraph_GraphMLFormat(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	writeEntityRelationship(t, eng, "default", "PostgreSQL", "database", "Redis", "cache", "uses")

	g, err := eng.ExportGraph(context.Background(), "default", false)
	require.NoError(t, err)
	require.NotNil(t, g)

	graphML, err := FormatGraphGraphML(g)
	require.NoError(t, err)
	require.NotEmpty(t, graphML)

	// Verify XML preamble and structure.
	require.True(t, strings.HasPrefix(graphML, `<?xml version="1.0" encoding="UTF-8"?>`), "expected XML declaration")
	require.Contains(t, graphML, `<graphml`)
	require.Contains(t, graphML, `<graph`)

	// Should contain both entity names.
	require.Contains(t, graphML, "PostgreSQL")
	require.Contains(t, graphML, "Redis")

	// Should contain the edge.
	require.Contains(t, graphML, `<edge`)
	require.Contains(t, graphML, "uses")
}

func TestExportGraph_EmptyVault(t *testing.T) {
	eng, cleanup := testEnv(t)
	defer cleanup()

	g, err := eng.ExportGraph(context.Background(), "empty-vault", false)
	require.NoError(t, err)
	require.NotNil(t, g)
	require.Empty(t, g.Nodes, "expected no nodes for empty vault")
	require.Empty(t, g.Edges, "expected no edges for empty vault")

	// JSON-LD export of empty graph should still be valid JSON.
	jsonLD, err := FormatGraphJSONLD(g)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(jsonLD), &doc))
	graph, _ := doc["@graph"].([]any)
	require.Empty(t, graph)

	// GraphML export of empty graph should still be valid XML.
	graphML, err := FormatGraphGraphML(g)
	require.NoError(t, err)
	require.Contains(t, graphML, `<graphml`)
}
