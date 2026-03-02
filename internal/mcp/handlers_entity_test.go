package mcp

import (
	"context"
	"encoding/json"
	"testing"
)

type entityAggEngine struct{ fakeEngine }

func (e *entityAggEngine) GetEntityAggregate(_ context.Context, _, _ string, _ int) (*EntityAggregate, error) {
	return &EntityAggregate{
		Name:          "PostgreSQL",
		Type:          "database",
		Confidence:    0.9,
		MentionCount:  3,
		State:         "active",
		Engrams:       []EntityEngramSummary{},
		Relationships: []EntityRelSummary{},
		CoOccurring:   []EntityCoOccurrence{},
	}, nil
}

func (e *entityAggEngine) ListEntities(_ context.Context, _ string, _ int, _ string) ([]EntitySummary, error) {
	return []EntitySummary{
		{Name: "PostgreSQL", Type: "database", MentionCount: 5, State: "active"},
	}, nil
}

func TestHandleEntity_HappyPath(t *testing.T) {
	srv := New(":0", &entityAggEngine{}, "", nil)
	w := postRPC(t, srv, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"muninn_entity","arguments":{"vault":"default","name":"PostgreSQL"}}}`)
	if w.Code != 200 {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp JSONRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestHandleEntity_MissingName(t *testing.T) {
	srv := newTestServer()
	w := postRPC(t, srv, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"muninn_entity","arguments":{"vault":"default"}}}`)
	var resp JSONRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestHandleEntities_HappyPath(t *testing.T) {
	srv := New(":0", &entityAggEngine{}, "", nil)
	w := postRPC(t, srv, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"muninn_entities","arguments":{"vault":"default"}}}`)
	if w.Code != 200 {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp JSONRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestHandleEntities_NoVaultDefaultsToDefault(t *testing.T) {
	// When vault is omitted, the server defaults to "default" — no error expected.
	srv := New(":0", &entityAggEngine{}, "", nil)
	w := postRPC(t, srv, `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"muninn_entities","arguments":{}}}`)
	if w.Code != 200 {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp JSONRPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}
