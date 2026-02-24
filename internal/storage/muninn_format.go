package storage

import "time"

// MuninnMagic is the 8-byte magic header for .muninn archive files.
var MuninnMagic = [8]byte{'M', 'U', 'N', 'I', 'N', 'N', 'K', 'V'}

// MuninnFormatVersion is the binary format version embedded in the archive header.
const MuninnFormatVersion uint32 = 1

// MuninnSchemaVersion is the schema version stored in the manifest JSON.
const MuninnSchemaVersion = 1

// MuninnManifest is the JSON manifest stored as manifest.json inside a .muninn archive.
type MuninnManifest struct {
	MuninnVersion string    `json:"muninn_version"`
	SchemaVersion int       `json:"schema_version"`
	Vault         string    `json:"vault"`
	EmbedderModel string    `json:"embedder_model"`
	Dimension     int       `json:"dimension"`
	EngramCount   int64     `json:"engram_count"`
	CreatedAt     time.Time `json:"created_at"`
	ResetMetadata bool      `json:"reset_metadata"`
}

// ExportOpts controls the export behaviour.
type ExportOpts struct {
	EmbedderModel string
	Dimension     int
	ResetMetadata bool
}

// ImportOpts controls the import behaviour.
type ImportOpts struct {
	ResetMetadata     bool
	SkipCompatCheck   bool
	ExpectedModel     string
	ExpectedDimension int
}

// ExportResult summarises a completed export.
type ExportResult struct {
	EngramCount int64
	TotalKeys   int64
}
