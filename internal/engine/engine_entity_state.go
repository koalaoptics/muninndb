package engine

import (
	"context"
	"fmt"

	"github.com/scrypster/muninndb/internal/storage"
)

// SetEntityState sets the lifecycle state of a named entity.
// For state="merged", mergedInto must be the canonical entity name.
func (e *Engine) SetEntityState(ctx context.Context, entityName, state, mergedInto string) error {
	if entityName == "" {
		return fmt.Errorf("set_entity_state: entity_name is required")
	}

	// Get existing to preserve other fields.
	existing, err := e.store.GetEntityRecord(ctx, entityName)
	if err != nil {
		return fmt.Errorf("set_entity_state: read entity: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("set_entity_state: entity %q not found", entityName)
	}

	// Build updated record — UpsertEntityRecord will validate state and MergedInto consistency.
	record := storage.EntityRecord{
		Name:       entityName,
		State:      state,
		MergedInto: mergedInto,
		// Preserve existing fields.
		Type:       existing.Type,
		Confidence: existing.Confidence,
	}

	return e.store.UpsertEntityRecord(ctx, record, "mcp:entity_state")
}
