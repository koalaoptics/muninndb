package main

import (
	"fmt"
	"io"
	"os"
	"time"
)

// writeReport writes a human-readable evaluation report to w.
func writeReport(w io.Writer, p1 Phase1Result, p2 Phase2Result, mode string, corpusSize int, loadDur time.Duration) {
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "════════════════════════════════════════════════════════════════\n")
	fmt.Fprintf(w, "MUNINNDB BIBLE EVAL REPORT\n")
	fmt.Fprintf(w, "════════════════════════════════════════════════════════════════\n")
	fmt.Fprintf(w, "Mode:          %s\n", mode)
	fmt.Fprintf(w, "Corpus size:   %d verses\n", corpusSize)
	fmt.Fprintf(w, "Load time:     %v\n", loadDur.Round(time.Millisecond))
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "── Phase 1: Retrieval Quality ──────────────────────────────────\n")
	fmt.Fprintf(w, "Seeds evaluated:   %d\n", p1.SeedsEvaluated)
	fmt.Fprintf(w, "Avg cross-refs:    %.1f\n", p1.AvgCrossRefs)
	fmt.Fprintf(w, "Recall@10:         %.4f\n", p1.RecallAtK)
	fmt.Fprintf(w, "NDCG@10:           %.4f\n", p1.NDCGAtK)
	fmt.Fprintf(w, "\n")

	fmt.Fprintf(w, "── Phase 2: Cognitive Properties ──────────────────────────────\n")
	fmt.Fprintf(w, "Baseline NDCG:     %.4f\n", p2.BaselineNDCG)
	fmt.Fprintf(w, "Post-reading NDCG: %.4f\n", p2.PostReadingNDCG)
	fmt.Fprintf(w, "Post-decay NDCG:   %.4f\n", p2.PostDecayNDCG)
	cognitiveImproved := p2.PostReadingNDCG > p2.BaselineNDCG
	fmt.Fprintf(w, "Cognitive change:  %+.4f (%s)\n",
		p2.PostReadingNDCG-p2.BaselineNDCG,
		func() string {
			if cognitiveImproved {
				return "improved"
			}
			return "no improvement"
		}())
	fmt.Fprintf(w, "\n")

	if len(p2.QueryDeltas) > 0 {
		fmt.Fprintf(w, "  Per-query deltas:\n")
		fmt.Fprintf(w, "  %-22s  %8s  %8s  %8s\n", "Label", "Baseline", "PostRead", "Delta")
		fmt.Fprintf(w, "  %-22s  %8s  %8s  %8s\n", "----------------------", "--------", "--------", "--------")
		for _, d := range p2.QueryDeltas {
			fmt.Fprintf(w, "  %-22s  %8.4f  %8.4f  %+8.4f\n", d.Label, d.BaselineNDCG, d.PostReadNDCG, d.Delta)
		}
		fmt.Fprintf(w, "\n")
	}

	fmt.Fprintf(w, "── Verdict ─────────────────────────────────────────────────────\n")
	fmt.Fprintf(w, "%s\n", verdictLine(p1.NDCGAtK, cognitiveImproved))
	fmt.Fprintf(w, "════════════════════════════════════════════════════════════════\n")
}

// verdictLine returns a one-line summary verdict based on NDCG and cognitive improvement.
func verdictLine(ndcg float64, cognitiveImproved bool) string {
	quality := "POOR"
	switch {
	case ndcg >= 0.5:
		quality = "HIGH"
	case ndcg >= 0.3:
		quality = "GOOD"
	case ndcg >= 0.1:
		quality = "ACCEPTABLE"
	}

	cog := "no Hebbian signal"
	if cognitiveImproved {
		cog = "Hebbian learning confirmed"
	}

	return fmt.Sprintf("Retrieval quality: %s (NDCG@10=%.4f) | Cognitive: %s", quality, ndcg, cog)
}

// saveReport writes the report to a file, appending if it already exists.
func saveReport(path string, p1 Phase1Result, p2 Phase2Result, mode string, corpusSize int, loadDur time.Duration) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open results file: %w", err)
	}
	defer f.Close()
	fmt.Fprintf(f, "\n# Run: %s\n", time.Now().Format(time.RFC3339))
	writeReport(f, p1, p2, mode, corpusSize, loadDur)
	return nil
}
