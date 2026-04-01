package updater

import (
	"encoding/json"
	"testing"
)

func TestManifestUnmarshal(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"latest_version": "0.2.0",
		"min_version": "0.1.0",
		"release_notes": "stability improvements",
		"assets": {
			"linux-amd64": {
				"download_url": "https://update.xfo.network/releases/v0.2.0/xfo-miner-linux-amd64.tar.gz",
				"checksum": "abc123",
				"filename": "xfo-miner-linux-amd64.tar.gz"
			}
		}
	}`)

	var manifest LatestManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}

	if manifest.LatestVersion != "0.2.0" {
		t.Fatalf("unexpected latest version: %q", manifest.LatestVersion)
	}
	if manifest.MinVersion != "0.1.0" {
		t.Fatalf("unexpected min version: %q", manifest.MinVersion)
	}
	if manifest.ReleaseNotes != "stability improvements" {
		t.Fatalf("unexpected release notes: %q", manifest.ReleaseNotes)
	}

	asset, ok := manifest.Assets["linux-amd64"]
	if !ok {
		t.Fatalf("expected linux-amd64 asset to exist")
	}
	if asset.DownloadURL == "" || asset.Checksum == "" || asset.Filename == "" {
		t.Fatalf("asset fields should not be empty: %#v", asset)
	}
}
