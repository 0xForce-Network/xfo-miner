package multisig

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStakingManagerInitiateStaking(t *testing.T) {
	t.Parallel()

	walletServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		switch req["method"] {
		case "prepare_multisig":
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"multisig_info": "miner-info"}})
		case "make_multisig":
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"multisig_info": "round2"}})
		case "exchange_multisig_keys":
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"address": "4abc"}})
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer walletServer.Close()

	oracleServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success":       true,
			"multisig_info": "oracle-info",
			"address":       "4oracle",
		})
	}))
	defer oracleServer.Close()

	manager := NewStakingManager(StakingConfig{WalletRPCURL: walletServer.URL, OracleAPIURL: oracleServer.URL}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	address, err := manager.InitiateStaking(context.Background())
	if err != nil {
		t.Fatalf("InitiateStaking() error = %v", err)
	}
	if address != "4abc" {
		t.Fatalf("unexpected multisig address: %s", address)
	}
}
