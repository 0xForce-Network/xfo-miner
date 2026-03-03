package multisig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type WalletRPCClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewWalletRPCClient(baseURL string) *WalletRPCClient {
	return &WalletRPCClient{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *WalletRPCClient) PrepareMultisig(ctx context.Context) (string, error) {
	var resp struct {
		MultisigInfo string `json:"multisig_info"`
	}
	if err := c.call(ctx, "prepare_multisig", nil, &resp); err != nil {
		return "", err
	}
	if resp.MultisigInfo == "" {
		return "", fmt.Errorf("prepare_multisig returned empty multisig_info")
	}
	return resp.MultisigInfo, nil
}

func (c *WalletRPCClient) MakeMultisig(ctx context.Context, multisigInfos []string, threshold uint) (string, error) {
	var resp struct {
		Address      string `json:"address"`
		MultisigInfo string `json:"multisig_info"`
	}
	params := map[string]any{"multisig_info": multisigInfos, "threshold": threshold}
	if err := c.call(ctx, "make_multisig", params, &resp); err != nil {
		return "", err
	}
	if resp.MultisigInfo != "" {
		return resp.MultisigInfo, nil
	}
	if resp.Address == "" {
		return "", fmt.Errorf("make_multisig returned empty response")
	}
	return resp.Address, nil
}

func (c *WalletRPCClient) ExchangeMultisigKeys(ctx context.Context, multisigInfos []string) (string, error) {
	var resp struct {
		Address string `json:"address"`
	}
	params := map[string]any{"multisig_info": multisigInfos}
	if err := c.call(ctx, "exchange_multisig_keys", params, &resp); err != nil {
		return "", err
	}
	if resp.Address == "" {
		return "", fmt.Errorf("exchange_multisig_keys returned empty address")
	}
	return resp.Address, nil
}

func (c *WalletRPCClient) call(ctx context.Context, method string, params any, result any) error {
	payload := map[string]any{"jsonrpc": "2.0", "id": "0", "method": method}
	if params != nil {
		payload["params"] = params
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal rpc request: %w", err)
	}
	targetURL := c.baseURL
	if !strings.HasSuffix(targetURL, "/json_rpc") {
		targetURL += "/json_rpc"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rpc request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("rpc request returned status %d", resp.StatusCode)
	}
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return fmt.Errorf("decode rpc response: %w", err)
	}
	if envelope.Error != nil {
		return fmt.Errorf("rpc error %d: %s", envelope.Error.Code, envelope.Error.Message)
	}
	if result == nil {
		return nil
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		return fmt.Errorf("decode rpc result: %w", err)
	}
	return nil
}
