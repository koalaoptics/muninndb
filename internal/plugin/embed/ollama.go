package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type OllamaProvider struct {
	client  *http.Client
	baseURL string
	model   string
}

type ollamaEmbedRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbedResponse struct {
	Embedding []float64 `json:"embedding"`
}

type ollamaShowRequest struct {
	Name string `json:"name"`
}

type ollamaShowResponse struct {
	ModelInfo map[string]any `json:"model_info"`
}

func (p *OllamaProvider) Name() string {
	return "ollama"
}

func (p *OllamaProvider) Init(ctx context.Context, cfg ProviderHTTPConfig) (int, error) {
	p.baseURL = cfg.BaseURL
	p.model = cfg.Model

	// Create HTTP client with 30s timeout (local, may be loading model)
	transport := &http.Transport{
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	p.client = &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	// Probe connectivity with root GET
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(probeCtx, "GET", p.baseURL, nil)
	if err != nil {
		return 0, fmt.Errorf("cannot connect to Ollama at %s — is it running? (%w)", p.baseURL, err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("cannot connect to Ollama at %s — is it running? (%w)", p.baseURL, err)
	}
	resp.Body.Close()

	// Probe model info to warn about small context windows.
	// A short context window causes long engrams to be silently truncated.
	p.probeContextLength(ctx)

	// Embed probe text to detect dimension
	embedCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	body, _ := json.Marshal(ollamaEmbedRequest{
		Model:  p.model,
		Prompt: "dimension detection probe",
	})

	embedReq, err := http.NewRequestWithContext(embedCtx, "POST",
		p.baseURL+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("cannot create embed request: %w", err)
	}
	embedReq.Header.Set("Content-Type", "application/json")

	embedResp, err := p.client.Do(embedReq)
	if err != nil {
		return 0, fmt.Errorf("cannot connect to Ollama at %s — is it running? (%w)", p.baseURL, err)
	}
	defer embedResp.Body.Close()

	if embedResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(embedResp.Body)
		return 0, fmt.Errorf("Ollama returned status %d: %s", embedResp.StatusCode, string(bodyBytes))
	}

	var ollamaResp ollamaEmbedResponse
	if err := json.NewDecoder(embedResp.Body).Decode(&ollamaResp); err != nil {
		return 0, fmt.Errorf("failed to decode Ollama response: %w", err)
	}

	if len(ollamaResp.Embedding) == 0 {
		return 0, fmt.Errorf("Ollama returned empty embedding")
	}

	dim := len(ollamaResp.Embedding)
	slog.Info("Ollama dimension probe successful", "dimension", dim)

	return dim, nil
}

func (p *OllamaProvider) EmbedBatch(ctx context.Context, texts []string) ([]float32, error) {
	// Ollama embeds one text at a time — loop and concatenate
	result := make([]float32, 0)

	for _, text := range texts {
		body, _ := json.Marshal(ollamaEmbedRequest{
			Model:  p.model,
			Prompt: text,
		})

		req, err := http.NewRequestWithContext(ctx, "POST",
			p.baseURL+"/api/embeddings", bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("ollama embed: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := p.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("ollama embed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("Ollama returned status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		var ollamaResp ollamaEmbedResponse
		if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
			return nil, fmt.Errorf("ollama decode: %w", err)
		}

		for _, v := range ollamaResp.Embedding {
			result = append(result, float32(v))
		}
	}

	return result, nil
}

func (p *OllamaProvider) MaxBatchSize() int {
	// Ollama embeds one at a time
	return 1
}

// probeContextLength queries /api/show for model info and logs a warning when
// the model's context window is below 2048 tokens, which risks silent truncation
// of longer engram content.
func (p *OllamaProvider) probeContextLength(ctx context.Context) {
	showCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	body, _ := json.Marshal(ollamaShowRequest{Name: p.model})
	req, err := http.NewRequestWithContext(showCtx, "POST", p.baseURL+"/api/show", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var show ollamaShowResponse
	if err := json.NewDecoder(resp.Body).Decode(&show); err != nil {
		return
	}

	// The context length is stored under a model-family-specific key in model_info.
	// Common keys: "llama.context_length", "bert.context_length", etc.
	const warnThreshold = 2048
	for k, v := range show.ModelInfo {
		if k != "llama.context_length" && k != "bert.context_length" &&
			k != "nomic_bert.context_length" && k != "qwen2.context_length" {
			continue
		}
		switch n := v.(type) {
		case float64:
			if int(n) < warnThreshold {
				slog.Warn("Ollama model has a small context window — long engrams may be truncated",
					"model", p.model,
					"context_length", int(n),
					"recommended_minimum", warnThreshold)
			} else {
				slog.Info("Ollama context length", "model", p.model, "context_length", int(n))
			}
		}
		return
	}
}

func (p *OllamaProvider) Close() error {
	if p.client != nil {
		p.client.CloseIdleConnections()
	}
	return nil
}
