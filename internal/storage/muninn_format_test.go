package storage_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/scrypster/muninndb/internal/storage"
)

func TestMuninnManifestRoundtrip(t *testing.T) {
	m := storage.MuninnManifest{
		MuninnVersion: "1",
		SchemaVersion: 1,
		Vault:         "test-vault",
		EmbedderModel: "all-MiniLM-L6-v2",
		Dimension:     384,
		EngramCount:   1234,
		CreatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ResetMetadata: false,
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(m); err != nil {
		t.Fatalf("encode: %v", err)
	}
	var got storage.MuninnManifest
	if err := json.NewDecoder(&buf).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Vault != m.Vault {
		t.Errorf("Vault: got %q, want %q", got.Vault, m.Vault)
	}
	if got.Dimension != m.Dimension {
		t.Errorf("Dimension: got %d, want %d", got.Dimension, m.Dimension)
	}
	if got.EngramCount != m.EngramCount {
		t.Errorf("EngramCount: got %d, want %d", got.EngramCount, m.EngramCount)
	}
}

func TestMuninnMagicBytes(t *testing.T) {
	if string(storage.MuninnMagic[:]) != "MUNINNKV" {
		t.Errorf("magic = %q, want MUNINNKV", storage.MuninnMagic)
	}
}
