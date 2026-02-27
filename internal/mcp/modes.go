package mcp

import "github.com/scrypster/muninndb/internal/auth"

// RecallMode is the MCP-internal adapter for recall mode presets.
// Fields with zero values are not applied (caller defaults remain).
type RecallMode struct {
	MaxHops   int
	Threshold float32
	// Scoring weight hints (applied to mbp.ActivateRequest if non-zero)
	SemanticSimilarity float32
	FullTextRelevance  float32
	Recency            float32
	DisableACTR        bool
}

// lookupMode returns the RecallMode for the given name by delegating to auth.LookupRecallMode.
func lookupMode(name string) (RecallMode, error) {
	p, err := auth.LookupRecallMode(name)
	if err != nil {
		return RecallMode{}, err
	}
	return RecallMode{
		MaxHops:            p.MaxHops,
		Threshold:          p.Threshold,
		SemanticSimilarity: p.SemanticSimilarity,
		FullTextRelevance:  p.FullTextRelevance,
		Recency:            p.Recency,
		DisableACTR:        p.DisableACTR,
	}, nil
}
