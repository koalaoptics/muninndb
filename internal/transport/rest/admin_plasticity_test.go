package rest

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/scrypster/muninndb/internal/auth"
)

func TestAdminPlasticity_TraversalProfile_Valid(t *testing.T) {
	profiles := []string{"default", "causal", "confirmatory", "adversarial", "structural"}

	for _, profile := range profiles {
		profile := profile
		t.Run(profile, func(t *testing.T) {
			as := newTestAuthStore(t)
			server := newTestServer(t, as)

			p := profile
			cfg := auth.PlasticityConfig{
				Preset:           "default",
				TraversalProfile: &p,
			}
			body, _ := json.Marshal(cfg)
			req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
			req.SetPathValue("name", "myvault")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			server.handlePutVaultPlasticity(as)(w, req)

			if w.Code != 200 {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}
			var resp struct {
				Resolved auth.ResolvedPlasticity `json:"resolved"`
			}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Resolved.TraversalProfile != profile {
				t.Errorf("expected TraversalProfile %q, got %q", profile, resp.Resolved.TraversalProfile)
			}
		})
	}
}

func TestAdminPlasticity_TraversalProfile_Invalid(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	profile := "nonexistent_profile"
	cfg := auth.PlasticityConfig{
		Preset:           "default",
		TraversalProfile: &profile,
	}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminPlasticity_TraversalProfile_Omitted(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	cfg := auth.PlasticityConfig{
		Preset: "default",
	}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleGetVaultPlasticity_DefaultWhenNil(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	req := httptest.NewRequest("GET", "/api/admin/vault/myvault/plasticity", nil)
	req.SetPathValue("name", "myvault")
	w := httptest.NewRecorder()
	server.handleGetVaultPlasticity(as)(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Raw      *auth.PlasticityConfig  `json:"config"`
		Resolved auth.ResolvedPlasticity `json:"resolved"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Raw != nil {
		t.Error("config should be nil when no Plasticity set")
	}
	if resp.Resolved.HopDepth != 2 {
		t.Errorf("resolved HopDepth want 2, got %d", resp.Resolved.HopDepth)
	}
}

func TestHandleGetVaultPlasticity_MissingName(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	req := httptest.NewRequest("GET", "/api/admin/vault//plasticity", nil)
	req.SetPathValue("name", "")
	w := httptest.NewRecorder()
	server.handleGetVaultPlasticity(as)(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePutVaultPlasticity_RoundTrip(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	body, _ := json.Marshal(auth.PlasticityConfig{Preset: "scratchpad"})
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 200 {
		t.Fatalf("PUT: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// GET and verify persisted
	req2 := httptest.NewRequest("GET", "/api/admin/vault/myvault/plasticity", nil)
	req2.SetPathValue("name", "myvault")
	w2 := httptest.NewRecorder()
	server.handleGetVaultPlasticity(as)(w2, req2)

	var resp struct {
		Raw      *auth.PlasticityConfig  `json:"config"`
		Resolved auth.ResolvedPlasticity `json:"resolved"`
	}
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("decode GET after PUT: %v", err)
	}
	if resp.Raw == nil || resp.Raw.Preset != "scratchpad" {
		t.Errorf("expected scratchpad preset, got %+v", resp.Raw)
	}
	if resp.Resolved.HopDepth != 0 {
		t.Errorf("scratchpad HopDepth want 0, got %d", resp.Resolved.HopDepth)
	}
}

func TestHandlePutVaultPlasticity_InvalidPreset(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	body, _ := json.Marshal(auth.PlasticityConfig{Preset: "invalid"})
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePutVaultPlasticity_OverrideRoundTrip(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	hopDepth := 5
	cfg := auth.PlasticityConfig{
		Preset:   "default",
		HopDepth: &hopDepth,
	}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	req2 := httptest.NewRequest("GET", "/api/admin/vault/myvault/plasticity", nil)
	req2.SetPathValue("name", "myvault")
	w2 := httptest.NewRecorder()
	server.handleGetVaultPlasticity(as)(w2, req2)

	var resp struct {
		Resolved auth.ResolvedPlasticity `json:"resolved"`
	}
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Resolved.HopDepth != 5 {
		t.Errorf("override HopDepth want 5, got %d", resp.Resolved.HopDepth)
	}
	// Other fields should come from default preset
	if !resp.Resolved.HebbianEnabled {
		t.Error("HebbianEnabled should be true (from default preset)")
	}
}

// --- RecallMode validation tests ---

func TestAdminPlasticity_RecallMode_Valid(t *testing.T) {
	modes := []string{"semantic", "recent", "balanced", "deep"}
	for _, mode := range modes {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			as := newTestAuthStore(t)
			server := newTestServer(t, as)

			m := mode
			cfg := auth.PlasticityConfig{
				Preset:     "default",
				RecallMode: &m,
			}
			body, _ := json.Marshal(cfg)
			req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
			req.SetPathValue("name", "myvault")
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			server.handlePutVaultPlasticity(as)(w, req)

			if w.Code != 200 {
				t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
			}
			var resp struct {
				Resolved auth.ResolvedPlasticity `json:"resolved"`
			}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Resolved.RecallMode != mode {
				t.Errorf("expected RecallMode %q, got %q", mode, resp.Resolved.RecallMode)
			}
		})
	}
}

func TestAdminPlasticity_RecallMode_Invalid(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	mode := "turbo"
	cfg := auth.PlasticityConfig{
		Preset:     "default",
		RecallMode: &mode,
	}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 400 {
		t.Errorf("expected 400 for invalid recall_mode, got %d: %s", w.Code, w.Body.String())
	}
}

func TestAdminPlasticity_RecallMode_Omitted(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	cfg := auth.PlasticityConfig{Preset: "default"}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Resolved auth.ResolvedPlasticity `json:"resolved"`
	}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Resolved.RecallMode != "balanced" {
		t.Errorf("omitted recall_mode should default to balanced, got %q", resp.Resolved.RecallMode)
	}
}

func TestAdminPlasticity_RecallMode_RoundTrip(t *testing.T) {
	as := newTestAuthStore(t)
	server := newTestServer(t, as)

	mode := "deep"
	cfg := auth.PlasticityConfig{
		Preset:     "default",
		RecallMode: &mode,
	}
	body, _ := json.Marshal(cfg)
	req := httptest.NewRequest("PUT", "/api/admin/vault/myvault/plasticity", bytes.NewReader(body))
	req.SetPathValue("name", "myvault")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.handlePutVaultPlasticity(as)(w, req)
	if w.Code != 200 {
		t.Fatalf("PUT: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// GET and verify persisted
	req2 := httptest.NewRequest("GET", "/api/admin/vault/myvault/plasticity", nil)
	req2.SetPathValue("name", "myvault")
	w2 := httptest.NewRecorder()
	server.handleGetVaultPlasticity(as)(w2, req2)

	var resp struct {
		Config   *auth.PlasticityConfig  `json:"config"`
		Resolved auth.ResolvedPlasticity `json:"resolved"`
	}
	if err := json.NewDecoder(w2.Body).Decode(&resp); err != nil {
		t.Fatalf("decode GET: %v", err)
	}
	if resp.Config == nil || resp.Config.RecallMode == nil || *resp.Config.RecallMode != "deep" {
		t.Errorf("persisted config recall_mode should be 'deep', got %+v", resp.Config)
	}
	if resp.Resolved.RecallMode != "deep" {
		t.Errorf("resolved RecallMode should be 'deep', got %q", resp.Resolved.RecallMode)
	}
}
