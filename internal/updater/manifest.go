package updater

type PlatformAsset struct {
	DownloadURL string `json:"download_url"`
	Checksum    string `json:"checksum"`
	Filename    string `json:"filename"`
}

type LatestManifest struct {
	LatestVersion string                   `json:"latest_version"`
	MinVersion    string                   `json:"min_version"`
	ReleaseNotes  string                   `json:"release_notes"`
	Assets        map[string]PlatformAsset `json:"assets"`
}