package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/scrypster/muninndb/internal/auth"
)

// vaultTrackingEngine wraps MockEngine and records the vault passed to key engine calls.
type vaultTrackingEngine struct {
	MockEngine
	lastWriteVault    string
	lastActivateVault string
	lastListVault     string
	lastReadVault     string
	lastForgetVault   string
}

func (e *vaultTrackingEngine) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	e.lastWriteVault = req.Vault
	return e.MockEngine.Write(ctx, req)
}

func (e *vaultTrackingEngine) Activate(ctx context.Context, req *ActivateRequest) (*ActivateResponse, error) {
	e.lastActivateVault = req.Vault
	return e.MockEngine.Activate(ctx, req)
}

func (e *vaultTrackingEngine) ListEngrams(ctx context.Context, req *ListEngramsRequest) (*ListEngramsResponse, error) {
	e.lastListVault = req.Vault
	return e.MockEngine.ListEngrams(ctx, req)
}

func (e *vaultTrackingEngine) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	e.lastReadVault = req.Vault
	return e.MockEngine.Read(ctx, req)
}

func (e *vaultTrackingEngine) Forget(ctx context.Context, req *ForgetRequest) (*ForgetResponse, error) {
	e.lastForgetVault = req.Vault
	return e.MockEngine.Forget(ctx, req)
}

// newVaultTrackingServer creates a Server with a vaultTrackingEngine and a
// public "default" vault. The store is returned so tests can configure auth.
func newVaultTrackingServer(t *testing.T) (*Server, *vaultTrackingEngine, *auth.Store) {
	t.Helper()
	eng := &vaultTrackingEngine{}
	store := newTestAuthStore(t)
	if err := store.SetVaultConfig(auth.VaultConfig{Name: "default", Public: true}); err != nil {
		t.Fatalf("SetVaultConfig: %v", err)
	}
	srv := NewServer("localhost:0", eng, store, nil, nil, EmbedInfo{}, EnrichInfo{}, nil, "", nil)
	return srv, eng, store
}

// TestCtxVault_NoContext verifies ctxVault falls back to "default" when no vault in context.
func TestCtxVault_NoContext(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/engrams", nil)
	got := ctxVault(req)
	if got != "default" {
		t.Errorf("ctxVault no context: want %q, got %q", "default", got)
	}
}

// TestVaultRouting_Write_DefaultVault verifies that POST /api/engrams with no
// vault param passes "default" to the engine.
func TestVaultRouting_Write_DefaultVault(t *testing.T) {
	srv, eng, _ := newVaultTrackingServer(t)

	body := strings.NewReader(`{"concept":"test","content":"hello"}`)
	req := httptest.NewRequest("POST", "/api/engrams", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if eng.lastWriteVault != "default" {
		t.Errorf("engine Write vault: want %q, got %q", "default", eng.lastWriteVault)
	}
}

// TestVaultRouting_Write_ExplicitVault verifies that POST /api/engrams?vault=myvault
// passes "myvault" to the engine.
func TestVaultRouting_Write_ExplicitVault(t *testing.T) {
	srv, eng, store := newVaultTrackingServer(t)
	if err := store.SetVaultConfig(auth.VaultConfig{Name: "myvault", Public: true}); err != nil {
		t.Fatalf("SetVaultConfig: %v", err)
	}

	body := strings.NewReader(`{"concept":"test","content":"hello"}`)
	req := httptest.NewRequest("POST", "/api/engrams?vault=myvault", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if eng.lastWriteVault != "myvault" {
		t.Errorf("engine Write vault: want %q, got %q", "myvault", eng.lastWriteVault)
	}
}

// TestVaultRouting_Activate_ExplicitVault verifies that POST /api/activate?vault=myvault
// passes "myvault" to the engine.
func TestVaultRouting_Activate_ExplicitVault(t *testing.T) {
	srv, eng, store := newVaultTrackingServer(t)
	if err := store.SetVaultConfig(auth.VaultConfig{Name: "myvault", Public: true}); err != nil {
		t.Fatalf("SetVaultConfig: %v", err)
	}

	body := strings.NewReader(`{"context":["something"]}`)
	req := httptest.NewRequest("POST", "/api/activate?vault=myvault", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if eng.lastActivateVault != "myvault" {
		t.Errorf("engine Activate vault: want %q, got %q", "myvault", eng.lastActivateVault)
	}
}

// TestVaultRouting_ListEngrams_ExplicitVault verifies that GET /api/engrams?vault=myvault
// passes "myvault" to the engine.
func TestVaultRouting_ListEngrams_ExplicitVault(t *testing.T) {
	srv, eng, store := newVaultTrackingServer(t)
	if err := store.SetVaultConfig(auth.VaultConfig{Name: "myvault", Public: true}); err != nil {
		t.Fatalf("SetVaultConfig: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/engrams?vault=myvault", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if eng.lastListVault != "myvault" {
		t.Errorf("engine ListEngrams vault: want %q, got %q", "myvault", eng.lastListVault)
	}
}
