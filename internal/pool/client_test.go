package pool

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestConnectAndLogin(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{}
	received := make(chan LoginMessage, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}

		var msg LoginMessage
		if json.Unmarshal(payload, &msg) == nil {
			received <- msg
		}
	}))
	defer server.Close()

	client := NewWSSClient(testLogger(), WithHeartbeatInterval(time.Hour), WithPingInterval(time.Hour))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Connect(ctx, wsURL(server.URL)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	login := &LoginMessage{
		NodeID:     "node-1",
		WorkerName: "worker-a",
		Version:    "0.1.0",
		OS:         "linux-amd64",
		Capabilities: &CapabilitiesData{
			HasGPU:       true,
			GPUCount:     1,
			HasHashcat:   true,
			HasDocker:    true,
			BenchmarkKHs: 1234.5,
			RunMode:      "GPU_FULL",
		},
	}
	if err := client.SendLogin(login); err != nil {
		t.Fatalf("SendLogin() error = %v", err)
	}

	select {
	case got := <-received:
		if got.Type != "login" || got.NodeID != login.NodeID || got.Version != login.Version || got.OS != login.OS || got.Capabilities == nil || got.Capabilities.BenchmarkKHs != 1234.5 {
			t.Fatalf("unexpected login payload: %#v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting login payload")
	}
}

func TestHeartbeat(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{}
	heartbeatCount := int32(0)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		deadline := time.Now().Add(500 * time.Millisecond)
		_ = conn.SetReadDeadline(deadline)
		for {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var hb HeartbeatMessage
			if json.Unmarshal(payload, &hb) == nil && hb.Type == "heartbeat" {
				atomic.AddInt32(&heartbeatCount, 1)
				if atomic.LoadInt32(&heartbeatCount) >= 2 {
					return
				}
			}
		}
	}))
	defer server.Close()

	client := NewWSSClient(testLogger(), WithHeartbeatInterval(30*time.Millisecond), WithPingInterval(time.Hour))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx, wsURL(server.URL)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt32(&heartbeatCount) < 2 {
		t.Fatalf("expected at least 2 heartbeat messages, got %d", heartbeatCount)
	}
}

func TestOnMessageDispatch(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		msg := JobGPUMessage{Type: "job_gpu", JobID: "job-1", HashMode: 22000, Target: "hash", Skip: 0, Limit: 100}
		_ = conn.WriteJSON(msg)
		<-time.After(100 * time.Millisecond)
	}))
	defer server.Close()

	client := NewWSSClient(testLogger(), WithHeartbeatInterval(time.Hour), WithPingInterval(time.Hour))
	defer client.Close()

	got := make(chan string, 1)
	client.OnMessage(func(msgType string, _ json.RawMessage) {
		got <- msgType
	})

	if err := client.Connect(context.Background(), wsURL(server.URL)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	select {
	case msgType := <-got:
		if msgType != "job_gpu" {
			t.Fatalf("unexpected type: %s", msgType)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting callback")
	}
}

func TestReconnectOnDisconnect(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{}
	connections := int32(0)
	messages := make(chan string, 2)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		count := atomic.AddInt32(&connections, 1)
		if count == 1 {
			_ = conn.Close()
			return
		}

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		messages <- string(payload)
	}))
	defer server.Close()

	client := NewWSSClient(testLogger(), WithHeartbeatInterval(time.Hour), WithPingInterval(time.Hour), WithReconnect(20*time.Millisecond, 100*time.Millisecond))
	defer client.Close()

	if err := client.Connect(context.Background(), wsURL(server.URL)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	time.Sleep(120 * time.Millisecond)
	if err := client.SendHeartbeat(); err != nil {
		t.Fatalf("SendHeartbeat() error = %v", err)
	}

	select {
	case <-messages:
		if atomic.LoadInt32(&connections) < 2 {
			t.Fatalf("expected reconnect, connections=%d", connections)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting post-reconnect message")
	}
}

func TestGracefulClose(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSSClient(testLogger(), WithHeartbeatInterval(time.Hour), WithPingInterval(time.Hour))
	if err := client.Connect(context.Background(), wsURL(server.URL)); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if err := client.SendHeartbeat(); err == nil {
		t.Fatalf("expected send to fail after close")
	}
}
