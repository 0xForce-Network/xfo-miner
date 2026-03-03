package multisig

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type StakingConfig struct {
	WalletRPCURL string `json:"wallet_rpc_url"`
	OracleAPIURL string `json:"oracle_api_url"`
}

type StakingManager struct {
	walletRPC *WalletRPCClient
	oracleURL string
	logger    *slog.Logger
	client    *http.Client
}

func NewStakingManager(cfg StakingConfig, logger *slog.Logger) *StakingManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &StakingManager{
		walletRPC: NewWalletRPCClient(cfg.WalletRPCURL),
		oracleURL: strings.TrimRight(cfg.OracleAPIURL, "/"),
		logger:    logger,
		client:    &http.Client{Timeout: 20 * time.Second},
	}
}

func (m *StakingManager) InitiateStaking(ctx context.Context) (string, error) {
	minerInfo, err := m.walletRPC.PrepareMultisig(ctx)
	if err != nil {
		return "", fmt.Errorf("prepare multisig: %w", err)
	}

	reqBody := map[string]any{
		"request_id":    fmt.Sprintf("miner-%d", time.Now().UnixNano()),
		"context":       "miner_stake",
		"initiator":     "miner",
		"multisig_info": minerInfo,
		"timestamp":     time.Now().UTC(),
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.oracleURL+"/api/v1/multisig/init", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build oracle init request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("oracle init request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("oracle init returned status %d", resp.StatusCode)
	}

	var initResp struct {
		Success      bool   `json:"success"`
		MultisigInfo string `json:"multisig_info"`
		Address      string `json:"address"`
		Error        string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&initResp); err != nil {
		return "", fmt.Errorf("decode oracle init response: %w", err)
	}
	if !initResp.Success {
		if initResp.Error == "" {
			initResp.Error = "coordination failed"
		}
		return "", fmt.Errorf(initResp.Error)
	}

	localRoundInfo, err := m.walletRPC.MakeMultisig(ctx, []string{initResp.MultisigInfo}, 2)
	if err != nil {
		return "", fmt.Errorf("make multisig: %w", err)
	}
	address, err := m.walletRPC.ExchangeMultisigKeys(ctx, []string{localRoundInfo})
	if err != nil {
		return "", fmt.Errorf("exchange multisig keys: %w", err)
	}

	m.logger.Info("multisig staking initialized", "address", address)
	return address, nil
}
