package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/scrypster/muninndb/internal/transport/mbp"
)

// BenchmarkWrite_Sequential measures sequential single-engram write throughput.
func BenchmarkWrite_Sequential(b *testing.B) {
	eng, cleanup := testEnvTB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eng.Write(ctx, &mbp.WriteRequest{
			Vault:   "bench",
			Concept: fmt.Sprintf("concept-%d", i),
			Content: fmt.Sprintf("content for benchmark engram number %d with some realistic length text", i),
			Tags:    []string{"bench", "sequential"},
		})
		if err != nil {
			b.Fatalf("Write[%d]: %v", i, err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkWrite_Parallel measures parallel write throughput across goroutines.
func BenchmarkWrite_Parallel(b *testing.B) {
	eng, cleanup := testEnvTB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, err := eng.Write(ctx, &mbp.WriteRequest{
				Vault:   "bench-parallel",
				Concept: fmt.Sprintf("parallel-concept-%d", i),
				Content: fmt.Sprintf("parallel content %d with realistic length for memory encoding", i),
			})
			if err != nil {
				b.Errorf("Write: %v", err)
			}
			i++
		}
	})
	b.ReportAllocs()
}

// BenchmarkActivate_Cold measures activation latency with no prior writes.
func BenchmarkActivate_Cold(b *testing.B) {
	eng, cleanup := testEnvTB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eng.Activate(ctx, &mbp.ActivateRequest{
			Vault:      "bench-cold",
			Context:    []string{"test query for benchmarking cold activation"},
			MaxResults: 10,
			Threshold:  0.1,
		})
		if err != nil {
			b.Fatalf("Activate: %v", err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkActivate_WithResults measures activation latency after pre-populating
// the vault with 100 engrams. This tests the realistic hot path.
func BenchmarkActivate_WithResults(b *testing.B) {
	eng, cleanup := testEnvTB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 engrams
	for i := 0; i < 100; i++ {
		_, err := eng.Write(ctx, &mbp.WriteRequest{
			Vault:   "bench-hot",
			Concept: fmt.Sprintf("pre-populated concept %d", i),
			Content: fmt.Sprintf("pre-populated content about topic number %d", i),
			Tags:    []string{"prepop"},
		})
		if err != nil {
			b.Fatalf("pre-populate Write[%d]: %v", i, err)
		}
	}

	// Allow FTS to index
	// (use a short sleep — benchmarks can afford 300ms setup)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := eng.Activate(ctx, &mbp.ActivateRequest{
			Vault:      "bench-hot",
			Context:    []string{"populated topic concept"},
			MaxResults: 10,
			Threshold:  0.01,
		})
		if err != nil {
			b.Fatalf("Activate: %v", err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkWriteBatch_20 measures throughput of 20-item batch writes.
func BenchmarkWriteBatch_20(b *testing.B) {
	eng, cleanup := testEnvTB(b)
	defer cleanup()
	ctx := context.Background()

	reqs := make([]*mbp.WriteRequest, 20)
	for i := range reqs {
		reqs[i] = &mbp.WriteRequest{
			Vault:   "bench-batch",
			Concept: fmt.Sprintf("batch concept %d", i),
			Content: fmt.Sprintf("batch content for item %d with realistic length text", i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, errs := eng.WriteBatch(ctx, reqs)
		for j, err := range errs {
			if err != nil {
				b.Fatalf("WriteBatch item[%d]: %v", j, err)
			}
		}
	}
	b.ReportAllocs()
}
