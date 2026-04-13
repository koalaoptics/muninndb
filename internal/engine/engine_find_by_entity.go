package engine

import (
	"context"
	"fmt"

	"github.com/scrypster/muninndb/internal/storage"
)

// FindByEntityResult holds the paginated result of a find-by-entity query.
type FindByEntityResult struct {
	Engrams []*storage.Engram
	Total   int // total matching engrams (before pagination)
}

// FindByEntity returns engrams in vault that mention entityName,
// using the 0x23 reverse index for O(matches) lookup.
// Results are limited to limit entries (default 20, max 500).
// When offset > 0, the first offset matches are skipped (pagination).
func (e *Engine) FindByEntity(ctx context.Context, vault, entityName string, limit, offset int) (*FindByEntityResult, error) {
	if entityName == "" {
		return nil, fmt.Errorf("find_by_entity: entity_name is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	ws := e.store.ResolveVaultPrefix(vault)
	var all []*storage.Engram
	err := e.store.ScanEntityEngrams(ctx, entityName, func(gotWS [8]byte, id storage.ULID) error {
		if gotWS != ws {
			return nil // different vault — skip
		}
		eng, err := e.store.GetEngram(ctx, ws, id)
		if err != nil || eng == nil {
			return nil // skip missing/deleted
		}
		if eng.State == storage.StateSoftDeleted || eng.State == storage.StateArchived {
			return nil
		}
		all = append(all, eng)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("find_by_entity: scan: %w", err)
	}

	total := len(all)

	// Apply pagination.
	if offset >= total {
		return &FindByEntityResult{Engrams: []*storage.Engram{}, Total: total}, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return &FindByEntityResult{Engrams: all[offset:end], Total: total}, nil
}
