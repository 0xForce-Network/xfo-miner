package updater

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"time"

	"github.com/0xforce/xfo-miner/internal/pool"
)

const (
	DefaultManifestURL    = "https://update.xfo.network/releases/latest.json"
	DefaultPollInterval   = 4 * time.Hour
	DefaultPollJitterMax  = 30 * time.Minute
	defaultPollHTTPClient = 30 * time.Second
)

type Poller struct {
	cdnURL     string
	interval   time.Duration
	jitterMax  time.Duration
	currentVer Version
	client     *http.Client
	logger     *slog.Logger
	onUpdate   func(ctx context.Context, ota *pool.OTAUpdateMessage) error
}

func NewPoller(cdnURL string, interval, jitterMax time.Duration, currentVer Version, client *http.Client, logger *slog.Logger, onUpdate func(ctx context.Context, ota *pool.OTAUpdateMessage) error) *Poller {
	if cdnURL == "" {
		cdnURL = DefaultManifestURL
	}
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	if jitterMax < 0 {
		jitterMax = 0
	}
	if client == nil {
		client = &http.Client{Timeout: defaultPollHTTPClient}
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Poller{
		cdnURL:     cdnURL,
		interval:   interval,
		jitterMax:  jitterMax,
		currentVer: currentVer,
		client:     client,
		logger:     logger,
		onUpdate:   onUpdate,
	}
}

func (p *Poller) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := p.CheckOnce(ctx); err != nil {
			p.logger.Warn("OTA poller check failed", "error", err)
		}

		wait := p.interval + p.randomJitter()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

func (p *Poller) CheckOnce(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cdnURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("request manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected manifest status code: %d", resp.StatusCode)
	}

	var manifest LatestManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}

	latest, err := ParseVersion(manifest.LatestVersion)
	if err != nil {
		return fmt.Errorf("parse latest version %q: %w", manifest.LatestVersion, err)
	}
	if !p.currentVer.LessThan(latest) {
		return nil
	}

	asset, ok := manifest.Assets[CurrentPlatformKey()]
	if !ok {
		return fmt.Errorf("manifest missing asset for platform %q", CurrentPlatformKey())
	}
	if asset.DownloadURL == "" {
		return fmt.Errorf("manifest asset download_url is empty for platform %q", CurrentPlatformKey())
	}

	if p.onUpdate == nil {
		return nil
	}

	return p.onUpdate(ctx, &pool.OTAUpdateMessage{
		Type:          "update_required",
		LatestVersion: manifest.LatestVersion,
		DownloadURLs:  []string{asset.DownloadURL},
		Checksum:      asset.Checksum,
	})
}

func (p *Poller) randomJitter() time.Duration {
	if p.jitterMax <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(p.jitterMax)))
}
