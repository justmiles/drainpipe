// Package pluginmanager handles downloading, caching, and resolving Steampipe
// plugin binaries for out-of-process execution.
package pluginmanager

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/rs/zerolog"
)

const (
	// Steampipe publishes plugin OCI images under this GHCR path.
	ghcrHost    = "ghcr.io"
	ociRepoBase = "turbot/steampipe/plugins"
)

// Manager locates and downloads Steampipe plugin binaries.
type Manager struct {
	CacheDir string // e.g. ~/.drainpipe/plugins/
	Logger   zerolog.Logger
}

// NewManager creates a Manager with the given cache directory.
// If cacheDir is empty, defaults to ~/.drainpipe/plugins.
func NewManager(cacheDir string, logger zerolog.Logger) *Manager {
	if cacheDir == "" {
		home, _ := os.UserHomeDir()
		cacheDir = filepath.Join(home, ".drainpipe", "plugins")
	}
	return &Manager{CacheDir: cacheDir, Logger: logger}
}

// PluginRef identifies a Steampipe plugin.
type PluginRef struct {
	Org     string // e.g. "turbot"
	Name    string // e.g. "aws"
	Version string // e.g. "1.30.0" or "latest"
}

// ParsePluginRef parses a plugin specifier like "turbot/aws@1.30.0".
// Supported formats:
//   - "turbot/aws@1.30.0" → org=turbot, name=aws, version=1.30.0
//   - "turbot/aws"        → org=turbot, name=aws, version=latest
//   - "aws@1.30.0"        → org=turbot (default), name=aws, version=1.30.0
//   - "aws"               → org=turbot, name=aws, version=latest
func ParsePluginRef(spec string) (PluginRef, error) {
	ref := PluginRef{Org: "turbot", Version: "latest"}

	// Split version
	if idx := strings.Index(spec, "@"); idx >= 0 {
		ref.Version = strings.TrimPrefix(spec[idx+1:], "v")
		spec = spec[:idx]
	}

	// Split org/name
	parts := strings.SplitN(spec, "/", 2)
	switch len(parts) {
	case 2:
		ref.Org = parts[0]
		ref.Name = parts[1]
	case 1:
		ref.Name = parts[0]
	}

	if ref.Name == "" {
		return PluginRef{}, fmt.Errorf("empty plugin name in spec %q", spec)
	}
	return ref, nil
}

// PluginName returns the go-plugin Dispense name (e.g. "steampipe-plugin-aws").
func (r PluginRef) PluginName() string {
	return "steampipe-plugin-" + r.Name
}

// BinaryName returns the expected binary filename.
func (r PluginRef) BinaryName() string {
	return "steampipe-plugin-" + r.Name + ".plugin"
}

// EnsurePlugin locates or downloads the plugin binary, returning its path.
// Resolution order:
//  1. Drainpipe cache: ~/.drainpipe/plugins/<org>/<name>/<version>/
//  2. Steampipe install: ~/.steampipe/plugins/hub.steampipe.io/plugins/<org>/<name>@<version>/
//  3. Download from the Steampipe OCI registry (ghcr.io/turbot/steampipe/plugins/…)
func (m *Manager) EnsurePlugin(ref PluginRef) (string, error) {
	// 1. Check drainpipe cache
	cachedPath := m.cachePath(ref)
	if fileExists(cachedPath) {
		m.Logger.Debug().Str("path", cachedPath).Msg("plugin found in cache")
		return cachedPath, nil
	}

	// 2. Check steampipe install directory
	if steampipePath := m.steampipePath(ref); steampipePath != "" {
		m.Logger.Debug().Str("path", steampipePath).Msg("plugin found in steampipe directory")
		return steampipePath, nil
	}

	// 3. Download from OCI registry
	m.Logger.Info().
		Str("plugin", ref.Org+"/"+ref.Name).
		Str("version", ref.Version).
		Msg("downloading plugin from OCI registry")

	if err := m.download(ref, cachedPath); err != nil {
		return "", fmt.Errorf("downloading plugin %s/%s@%s: %w", ref.Org, ref.Name, ref.Version, err)
	}

	return cachedPath, nil
}

// EnsurePluginFromPath validates that a user-provided binary path exists and is executable.
func (m *Manager) EnsurePluginFromPath(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("plugin binary not found: %w", err)
	}
	if info.IsDir() {
		return "", fmt.Errorf("plugin path %q is a directory, not a binary", path)
	}
	if info.Mode()&0111 == 0 {
		return "", fmt.Errorf("plugin binary %q is not executable", path)
	}
	return path, nil
}

func (m *Manager) cachePath(ref PluginRef) string {
	return filepath.Join(m.CacheDir, ref.Org, ref.Name, ref.Version, ref.BinaryName())
}

func (m *Manager) steampipePath(ref PluginRef) string {
	home, _ := os.UserHomeDir()
	if home == "" {
		return ""
	}

	// Try exact version match
	path := filepath.Join(home, ".steampipe", "plugins", "hub.steampipe.io", "plugins",
		ref.Org, ref.Name+"@"+ref.Version, ref.BinaryName())
	if fileExists(path) {
		return path
	}

	// For "latest", try to find any installed version
	if ref.Version == "latest" {
		pattern := filepath.Join(home, ".steampipe", "plugins", "hub.steampipe.io", "plugins",
			ref.Org, ref.Name+"@*", ref.BinaryName())
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			return matches[len(matches)-1]
		}
	}

	return ""
}

// ociManifest is the subset of an OCI image manifest we need to locate layers.
type ociManifest struct {
	Layers []ociLayer `json:"layers"`
}

type ociLayer struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations"`
}

// download fetches the plugin binary from the Steampipe OCI registry (GHCR)
// using the standard OCI Distribution HTTP API, matching how `steampipe plugin
// install` resolves images.
func (m *Manager) download(ref PluginRef, destPath string) error {
	goos := runtime.GOOS
	goarch := runtime.GOARCH

	repo := fmt.Sprintf("%s/%s/%s", ociRepoBase, ref.Org, ref.Name)
	tag := ref.Version
	pluginMediaType := fmt.Sprintf(
		"application/vnd.turbot.steampipe.plugin.%s-%s.layer.v1+gzip", goos, goarch,
	)

	m.Logger.Debug().
		Str("registry", ghcrHost).
		Str("repo", repo).
		Str("tag", tag).
		Str("mediaType", pluginMediaType).
		Msg("pulling plugin from OCI registry")

	// 1. Anonymous auth token for GHCR
	token, err := m.ghcrToken(repo)
	if err != nil {
		return fmt.Errorf("obtaining registry token: %w", err)
	}

	// 2. Fetch OCI manifest
	manifest, err := m.fetchManifest(repo, tag, token)
	if err != nil {
		return fmt.Errorf("fetching OCI manifest for %s:%s: %w", repo, tag, err)
	}

	// 3. Find the plugin binary layer for this platform
	blobDigest := findLayerDigest(manifest.Layers, pluginMediaType)

	// Fallback: on darwin/arm64 try the amd64 layer (Steampipe does this for
	// plugins that haven't published arm64 builds yet).
	if blobDigest == "" && goos == "darwin" && goarch == "arm64" {
		fallback := fmt.Sprintf(
			"application/vnd.turbot.steampipe.plugin.%s-amd64.layer.v1+gzip", goos,
		)
		blobDigest = findLayerDigest(manifest.Layers, fallback)
		if blobDigest != "" {
			m.Logger.Warn().Msg("arm64 plugin binary not available, falling back to amd64")
		}
	}

	if blobDigest == "" {
		return fmt.Errorf("OCI image %s:%s has no plugin layer for %s/%s", repo, tag, goos, goarch)
	}

	// 4. Download the blob and decompress into the cache
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating cache directory: %w", err)
	}
	if err := m.downloadBlob(repo, blobDigest, token, destPath); err != nil {
		return fmt.Errorf("downloading plugin binary: %w", err)
	}

	m.Logger.Info().Str("path", destPath).Msg("plugin downloaded successfully")
	return nil
}

// ghcrToken obtains an anonymous pull token for a GHCR repository.
func (m *Manager) ghcrToken(repo string) (string, error) {
	url := fmt.Sprintf("https://%s/token?scope=repository:%s:pull", ghcrHost, repo)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request failed: HTTP %d", resp.StatusCode)
	}

	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	return body.Token, nil
}

// fetchManifest retrieves the OCI image manifest for the given repo:tag.
func (m *Manager) fetchManifest(repo, tag, token string) (*ociManifest, error) {
	url := fmt.Sprintf("https://%s/v2/%s/manifests/%s", ghcrHost, repo, tag)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.oci.image.manifest.v1+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d fetching manifest for %s:%s", resp.StatusCode, repo, tag)
	}

	var manifest ociManifest
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	return &manifest, nil
}

// downloadBlob fetches an OCI blob (the gzipped plugin binary) and
// decompresses it to destPath.
func (m *Manager) downloadBlob(repo, digest, token, destPath string) error {
	url := fmt.Sprintf("https://%s/v2/%s/blobs/%s", ghcrHost, repo, digest)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("blob download failed: HTTP %d for %s", resp.StatusCode, digest)
	}

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gz.Close()

	f, err := os.OpenFile(destPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("creating binary file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, gz); err != nil {
		os.Remove(destPath)
		return fmt.Errorf("writing binary: %w", err)
	}

	return nil
}

func findLayerDigest(layers []ociLayer, mediaType string) string {
	for _, l := range layers {
		if l.MediaType == mediaType {
			return l.Digest
		}
	}
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
