package opensearchmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Client representa o cliente para interação com OpenSearch
type Client struct {
	HTTPClient *http.Client
	Endpoint   string
	Username   string
	Password   string
}

// NewClient cria uma nova instância do cliente
func NewClient(endpoint, username, password string) *Client {
	return &Client{
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
		Endpoint:   endpoint,
		Username:   username,
		Password:   password,
	}
}

// doRequest executa requisições HTTP para a API do OpenSearch
func (c *Client) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.Endpoint+path, body)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(c.Username, c.Password)
	req.Header.Set("Content-Type", "application/json")

	return c.HTTPClient.Do(req)
}

// IndexInfo representa informações básicas de um índice
type IndexInfo struct {
	Name       string
	Status     string
	DocsCount  int64
	StoreSize  string
	CreateTime time.Time
}

// ListIndices retorna todos os índices no cluster
func (c *Client) ListIndices(ctx context.Context) ([]IndexInfo, error) {
	resp, err := c.doRequest(ctx, "GET", "/_cat/indices?format=json", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var indices []struct {
		Index      string `json:"index"`
		Status     string `json:"status"`
		DocsCount  string `json:"docs.count"`
		StoreSize  string `json:"store.size"`
		CreateTime string `json:"creation.date.string"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&indices); err != nil {
		return nil, err
	}

	var result []IndexInfo
	for _, idx := range indices {
		createTime, _ := time.Parse(time.RFC3339, idx.CreateTime)
		result = append(result, IndexInfo{
			Name:   idx.Index,
			Status: idx.Status,
			DocsCount: func(s string) int64 {
				val, _ := strconv.ParseInt(s, 10, 64)
				return val
			}(idx.DocsCount),
			StoreSize:  idx.StoreSize,
			CreateTime: createTime,
		})
	}

	return result, nil
}

// DeleteIndices exclui índices com base em um padrão de nome
func (c *Client) DeleteIndices(ctx context.Context, indexPattern string) error {
	// Primeiro verifica se existem índices que correspondem ao padrão
	indices, err := c.ListIndices(ctx)
	if err != nil {
		return err
	}

	var toDelete []string
	for _, idx := range indices {
		if matched, _ := filepath.Match(indexPattern, idx.Name); matched {
			toDelete = append(toDelete, idx.Name)
		}
	}

	if len(toDelete) == 0 {
		return fmt.Errorf("no indices match pattern: %s", indexPattern)
	}

	path := fmt.Sprintf("/%s", strings.Join(toDelete, ","))
	resp, err := c.doRequest(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete indices: %s", string(body))
	}

	return nil
}

// AliasAction representa uma ação de alias
type AliasAction struct {
	Add    map[string]interface{} `json:"add,omitempty"`
	Remove map[string]interface{} `json:"remove,omitempty"`
}

// ManageAliases gerencia operações de alias
func (c *Client) ManageAliases(ctx context.Context, actions []AliasAction) error {
	body := map[string]interface{}{
		"actions": actions,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/_aliases", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to manage aliases: %s", string(body))
	}

	return nil
}

// Exemplo de uso:
// actions := []AliasAction{
//     {
//         Add: map[string]interface{}{
//             "index": "myindex-2023.01.01",
//             "alias": "myindex-current",
//         },
//     },
//     {
//         Remove: map[string]interface{}{
//             "index": "myindex-2022.12.31",
//             "alias": "myindex-current",
//         },
//     },
// }
// client.ManageAliases(ctx, actions)

// Rollover executa uma operação de rollover em um alias
func (c *Client) Rollover(ctx context.Context, alias string, conditions map[string]interface{}) error {
	body := map[string]interface{}{
		"conditions": conditions,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("/%s/_rollover", alias)
	resp, err := c.doRequest(ctx, "POST", path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to rollover index: %s", string(body))
	}

	return nil
}

// Reindex executa uma operação de reindexação
func (c *Client) Reindex(ctx context.Context, source, dest string, query map[string]interface{}) error {
	body := map[string]interface{}{
		"source": map[string]interface{}{
			"index": source,
			"query": query,
		},
		"dest": map[string]interface{}{
			"index": dest,
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/_reindex", bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to reindex: %s", string(body))
	}

	return nil
}

// CloseIndices fecha índices que correspondem a um padrão
func (c *Client) CloseIndices(ctx context.Context, indexPattern string) error {
	indices, err := c.ListIndices(ctx)
	if err != nil {
		return err
	}

	var toClose []string
	for _, idx := range indices {
		if matched, _ := filepath.Match(indexPattern, idx.Name); matched {
			toClose = append(toClose, idx.Name)
		}
	}

	if len(toClose) == 0 {
		return fmt.Errorf("no indices match pattern: %s", indexPattern)
	}

	path := fmt.Sprintf("/%s/_close", strings.Join(toClose, ","))
	resp, err := c.doRequest(ctx, "POST", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to close indices: %s", string(body))
	}

	return nil
}

// funcionalidades adicionais
// CleanupByAge remove índices mais antigos que N dias
func (c *Client) CleanupByAge(ctx context.Context, indexPrefix string, days int) error {
	cutoff := time.Now().AddDate(0, 0, -days)
	indices, err := c.ListIndices(ctx)
	if err != nil {
		return err
	}

	var toDelete []string
	for _, idx := range indices {
		if strings.HasPrefix(idx.Name, indexPrefix) && idx.CreateTime.Before(cutoff) {
			toDelete = append(toDelete, idx.Name)
		}
	}

	if len(toDelete) == 0 {
		return nil
	}

	path := fmt.Sprintf("/%s", strings.Join(toDelete, ","))
	resp, err := c.doRequest(ctx, "DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete old indices: %s", string(body))
	}

	return nil
}

// OpenIndex abre um índice fechado
func (c *Client) OpenIndex(ctx context.Context, indexName string) error {
	path := fmt.Sprintf("/%s/_open", indexName)
	resp, err := c.doRequest(ctx, "POST", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to open index: %s", string(body))
	}
	return nil
}

// ShrinkIndex corrigido - agora com suporte completo
func (c *Client) ShrinkIndex(ctx context.Context, source, target string, settings map[string]interface{}) error {
	// 1. Fechar o índice fonte
	if err := c.CloseIndices(ctx, source); err != nil {
		return fmt.Errorf("failed to close source index: %w", err)
	}

	// 2. Configurar o shrink
	body := map[string]interface{}{
		"settings": mergeSettings(settings, map[string]interface{}{
			"index.blocks.write":       true,
			"index.number_of_replicas": 0,
			"index.number_of_shards":   settings["number_of_shards"],
		}),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal shrink settings: %w", err)
	}

	// 3. Executar o shrink
	path := fmt.Sprintf("/%s/_shrink/%s", source, target)
	resp, err := c.doRequest(ctx, "POST", path, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to execute shrink request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("shrink failed with status %d: %s", resp.StatusCode, string(body))
	}

	// 4. Reabrir os índices
	if err := c.OpenIndex(ctx, source); err != nil {
		return fmt.Errorf("failed to reopen source index: %w", err)
	}

	if err := c.OpenIndex(ctx, target); err != nil {
		return fmt.Errorf("failed to open target index: %w", err)
	}

	// 5. Aplicar configurações finais no novo índice
	finalSettings := map[string]interface{}{
		"index.number_of_replicas": settings["number_of_replicas"],
		"index.blocks.write":       nil, // Remove o bloqueio
	}

	if err := c.UpdateIndexSettings(ctx, target, finalSettings); err != nil {
		return fmt.Errorf("failed to apply final settings: %w", err)
	}

	return nil
}

// mergeSettings combina configurações de índices
func mergeSettings(base, override map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		result[k] = v
	}
	return result
}

// UpdateIndexSettings atualiza as configurações de um índice
func (c *Client) UpdateIndexSettings(ctx context.Context, indexName string, settings map[string]interface{}) error {
	body := map[string]interface{}{"settings": settings}
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("/%s/_settings", indexName)
	resp, err := c.doRequest(ctx, "PUT", path, bytes.NewReader(jsonBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update settings: %s", string(body))
	}
	return nil
}
