package multisig

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWalletRPCClientPrepareMultisig(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"multisig_info": "MultisigV1abc"},
		})
	}))
	defer srv.Close()

	client := NewWalletRPCClient(srv.URL)
	info, err := client.PrepareMultisig(context.Background())
	if err != nil {
		t.Fatalf("PrepareMultisig() error = %v", err)
	}
	if info != "MultisigV1abc" {
		t.Fatalf("unexpected multisig info: %s", info)
	}
}

func TestWalletRPCClientMakeAndExchange(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["method"] == "make_multisig" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{"multisig_info": "Round2Info"},
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{"address": "48abc"},
		})
	}))
	defer srv.Close()

	client := NewWalletRPCClient(srv.URL)
	info, err := client.MakeMultisig(context.Background(), []string{"m1", "m2"}, 2)
	if err != nil {
		t.Fatalf("MakeMultisig() error = %v", err)
	}
	if info != "Round2Info" {
		t.Fatalf("unexpected make multisig output: %s", info)
	}

	address, err := client.ExchangeMultisigKeys(context.Background(), []string{"Round2Info"})
	if err != nil {
		t.Fatalf("ExchangeMultisigKeys() error = %v", err)
	}
	if address != "48abc" {
		t.Fatalf("unexpected address: %s", address)
	}
}
