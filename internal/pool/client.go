package pool

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/0xforce/xfo-miner/internal/debuglog"
	"github.com/gorilla/websocket"
)

// Client defines basic pool connectivity contract.
type Client interface {
	Connect(ctx context.Context, url string) error
	Close() error
	SendLogin(login *LoginMessage) error
	SendHeartbeat() error
	SendProgress(msg *ProgressMessage) error
	SendResult(msg *ResultMessage) error
	SendProbeResult(msg *ProbeResultMessage) error
	SendContainerReady(msg *ContainerReadyMessage) error
	SendTelemetryL1(msg *TelemetryL1Message) error
	SendTelemetryL2(msg *TelemetryL2Message) error
	OnMessage(handler func(msgType string, raw json.RawMessage))
	OnReconnect(handler func())
}

type WSSClient struct {
	dialer *websocket.Dialer
	logger *slog.Logger

	heartbeatInterval time.Duration
	pingInterval      time.Duration
	reconnectBase     time.Duration
	reconnectMax      time.Duration

	mu        sync.RWMutex
	conn      *websocket.Conn
	callback  func(msgType string, raw json.RawMessage)
	reconnectCallback func()
	sendQueue chan []byte

	closeOnce sync.Once
	closeCh   chan struct{}
	doneCh    chan struct{}
	cancel    context.CancelFunc
	running   bool
}

type ClientOption func(*WSSClient)

func WithHeartbeatInterval(d time.Duration) ClientOption {
	return func(c *WSSClient) { c.heartbeatInterval = d }
}

func WithPingInterval(d time.Duration) ClientOption {
	return func(c *WSSClient) { c.pingInterval = d }
}

func WithReconnect(base, max time.Duration) ClientOption {
	return func(c *WSSClient) {
		c.reconnectBase = base
		c.reconnectMax = max
	}
}

// WithInsecureSkipVerify disables TLS certificate verification.
// Use ONLY for local testnet with self-signed certificates.
func WithInsecureSkipVerify() ClientOption {
	return func(c *WSSClient) {
		c.dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // testnet only
	}
}

func NewWSSClient(logger *slog.Logger, opts ...ClientOption) *WSSClient {
	if logger == nil {
		logger = slog.Default()
	}

	c := &WSSClient{
		dialer: &websocket.Dialer{HandshakeTimeout: 10 * time.Second},
		logger: logger,

		heartbeatInterval: 30 * time.Second,
		pingInterval:      30 * time.Second,
		reconnectBase:     1 * time.Second,
		reconnectMax:      30 * time.Second,

		sendQueue: make(chan []byte, 256),
		closeCh:   make(chan struct{}),
		doneCh:    make(chan struct{}),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c
}

func (c *WSSClient) Connect(ctx context.Context, url string) error {
	runCtx, cancel := context.WithCancel(ctx)
	conn, _, err := c.dialer.DialContext(runCtx, url, nil)
	if err != nil {
		cancel()
		return fmt.Errorf("dial websocket: %w", err)
	}

	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		_ = conn.Close()
		cancel()
		return errors.New("client already connected")
	}
	c.conn = conn
	c.cancel = cancel
	c.running = true
	c.mu.Unlock()

	go c.run(runCtx, url, conn)
	return nil
}

func (c *WSSClient) run(ctx context.Context, url string, initial *websocket.Conn) {
	defer close(c.doneCh)

	currentConn := initial
	backoff := c.reconnectBase

	for {
		err := c.runConnection(ctx, currentConn)
		if err != nil {
			c.logger.Warn("websocket connection ended", "error", err)
		}

		if c.isClosed() || ctx.Err() != nil {
			return
		}

		nextConn, reconnectErr := c.reconnect(ctx, url, backoff)
		if reconnectErr != nil {
			return
		}
		if nextConn == nil {
			return
		}

		currentConn = nextConn
		backoff = c.reconnectBase
	}
}

func (c *WSSClient) runConnection(ctx context.Context, conn *websocket.Conn) error {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)

	go c.readLoop(conn, errCh)
	go c.writeLoop(connCtx, conn, errCh)

	select {
	case err := <-errCh:
		_ = conn.Close()
		return err
	case <-ctx.Done():
		_ = conn.Close()
		return ctx.Err()
	case <-c.closeCh:
		_ = conn.Close()
		return nil
	}
}

func (c *WSSClient) reconnect(ctx context.Context, url string, initialBackoff time.Duration) (*websocket.Conn, error) {
	backoff := initialBackoff
	if backoff <= 0 {
		backoff = c.reconnectBase
	}

	for {
		nextConn, _, err := c.dialer.DialContext(ctx, url, nil)
		if err == nil {
			c.mu.Lock()
			c.conn = nextConn
			reconnectCallback := c.reconnectCallback
			c.mu.Unlock()
			if reconnectCallback != nil {
				go reconnectCallback()
			}
			return nextConn, nil
		}

		c.logger.Warn("reconnect failed", "error", err, "backoff", backoff)

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-c.closeCh:
			timer.Stop()
			return nil, context.Canceled
		}

		backoff *= 2
		if backoff > c.reconnectMax {
			backoff = c.reconnectMax
		}
	}
}

func (c *WSSClient) readLoop(conn *websocket.Conn, errCh chan<- error) {
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}

		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			continue
		}

		cb := c.getCallback()
		if cb != nil {
			cb(envelope.Type, payload)
		}
	}
}

func (c *WSSClient) writeLoop(ctx context.Context, conn *websocket.Conn, errCh chan<- error) {
	heartbeatTicker := time.NewTicker(c.heartbeatInterval)
	pingTicker := time.NewTicker(c.pingInterval)
	defer heartbeatTicker.Stop()
	defer pingTicker.Stop()

	for {
		select {
		case payload := <-c.sendQueue:
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				errCh <- err
				return
			}
		case <-heartbeatTicker.C:
			if err := c.writeJSON(conn, &HeartbeatMessage{Type: "heartbeat"}); err != nil {
				errCh <- err
				return
			}
			debuglog.Log("heartbeat_sent", "miner_id", debuglog.CurrentMinerID(), "timestamp", time.Now().Unix())
		case <-pingTicker.C:
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second)); err != nil {
				errCh <- err
				return
			}
		case <-ctx.Done():
			return
		case <-c.closeCh:
			return
		}
	}
}

func (c *WSSClient) writeJSON(conn *websocket.Conn, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func (c *WSSClient) SendLogin(login *LoginMessage) error {
	if login == nil {
		return errors.New("login message is nil")
	}
	if login.Type == "" {
		login.Type = "login"
	}
	debuglog.Log("login_enqueued", "devices_count", len(login.Devices), "worker_name", login.WorkerName)
	return c.enqueue(login)
}

func (c *WSSClient) SendHeartbeat() error {
	return c.enqueue(&HeartbeatMessage{Type: "heartbeat"})
}

func (c *WSSClient) SendProgress(msg *ProgressMessage) error {
	if msg == nil {
		return errors.New("progress message is nil")
	}
	if msg.Type == "" {
		msg.Type = "progress"
	}
	return c.enqueue(msg)
}

func (c *WSSClient) SendResult(msg *ResultMessage) error {
	if msg == nil {
		return errors.New("result message is nil")
	}
	if msg.Type == "" {
		msg.Type = "result"
	}
	return c.enqueue(msg)
}

func (c *WSSClient) SendProbeResult(msg *ProbeResultMessage) error {
	if msg == nil {
		return errors.New("probe_result message is nil")
	}
	if msg.Type == "" {
		msg.Type = "probe_result"
	}
	return c.enqueue(msg)
}

func (c *WSSClient) SendContainerReady(msg *ContainerReadyMessage) error {
	if msg == nil {
		return errors.New("container message is nil")
	}
	if msg.Type == "" {
		msg.Type = "container_ready"
	}
	return c.enqueue(msg)
}

func (c *WSSClient) SendTelemetryL1(msg *TelemetryL1Message) error {
	if msg == nil {
		return errors.New("telemetry l1 message is nil")
	}
	if msg.Type == "" {
		msg.Type = "telemetry_l1"
	}
	return c.enqueue(msg)
}

func (c *WSSClient) SendTelemetryL2(msg *TelemetryL2Message) error {
	if msg == nil {
		return errors.New("telemetry l2 message is nil")
	}
	if msg.Type == "" {
		msg.Type = "telemetry_l2"
	}
	debuglog.Log("telemetry_l2_enqueued", "devices_count", len(msg.Devices), "miner_id", debuglog.CurrentMinerID(), "job_id", msg.JobID)
	return c.enqueue(msg)
}

func (c *WSSClient) enqueue(msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if c.isClosed() {
		return errors.New("client is closed")
	}

	select {
	case c.sendQueue <- payload:
		return nil
	case <-c.closeCh:
		return errors.New("client is closed")
	default:
		return errors.New("send queue is full")
	}
}

func (c *WSSClient) OnMessage(handler func(msgType string, raw json.RawMessage)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callback = handler
}

func (c *WSSClient) getCallback() func(msgType string, raw json.RawMessage) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.callback
}

func (c *WSSClient) OnReconnect(handler func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconnectCallback = handler
}

func (c *WSSClient) isClosed() bool {
	select {
	case <-c.closeCh:
		return true
	default:
		return false
	}
}

func (c *WSSClient) Close() error {
	c.closeOnce.Do(func() {
		close(c.closeCh)

		c.mu.RLock()
		running := c.running
		c.mu.RUnlock()

		c.mu.Lock()
		if c.cancel != nil {
			c.cancel()
		}
		if c.conn != nil {
			_ = c.conn.Close()
			c.conn = nil
		}
		c.running = false
		c.mu.Unlock()

		if running {
			<-c.doneCh
		}
	})
	return nil
}

type NoopClient struct{}

func NewNoopClient() *NoopClient {
	return &NoopClient{}
}

func (c *NoopClient) Connect(_ context.Context, _ string) error {
	return nil
}

func (c *NoopClient) Close() error {
	return nil
}

func (c *NoopClient) SendLogin(_ *LoginMessage) error {
	return nil
}

func (c *NoopClient) SendHeartbeat() error {
	return nil
}

func (c *NoopClient) SendProgress(_ *ProgressMessage) error {
	return nil
}

func (c *NoopClient) SendResult(_ *ResultMessage) error {
	return nil
}

func (c *NoopClient) SendProbeResult(_ *ProbeResultMessage) error {
	return nil
}

func (c *NoopClient) SendContainerReady(_ *ContainerReadyMessage) error {
	return nil
}

func (c *NoopClient) SendTelemetryL1(_ *TelemetryL1Message) error {
	return nil
}

func (c *NoopClient) SendTelemetryL2(_ *TelemetryL2Message) error {
	return nil
}

func (c *NoopClient) OnMessage(_ func(msgType string, raw json.RawMessage)) {}

func (c *NoopClient) OnReconnect(_ func()) {}
