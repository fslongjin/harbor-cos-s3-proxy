package main

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func TestStripBucketPrefix(t *testing.T) {
	got, err := stripBucketPrefix("/example-bucket-1234567890/docker/registry/v2", "example-bucket-1234567890")
	if err != nil {
		t.Fatal(err)
	}
	if got != "/docker/registry/v2" {
		t.Fatalf("got %q", got)
	}
}

func TestTargetURL(t *testing.T) {
	p := proxyServer{
		cfg: config{
			Bucket:   "example-bucket-1234567890",
			Endpoint: "https://example-bucket-1234567890.cos.example-region.myqcloud.com",
		},
	}
	req := &http.Request{URL: mustURL(t, "http://127.0.0.1:19000/example-bucket-1234567890/docker/registry/v2?uploads=")}
	got, err := p.targetURL(req)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://example-bucket-1234567890.cos.example-region.myqcloud.com/docker/registry/v2?uploads="
	if got.String() != want {
		t.Fatalf("got %q, want %q", got.String(), want)
	}
}

func TestSignRequestUsesVirtualHost(t *testing.T) {
	u, err := url.Parse("https://example-bucket-1234567890.cos.example-region.myqcloud.com/docker/registry/v2/blobs/data")
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	p := proxyServer{
		cfg: config{
			Region:    "example-region",
			AccessKey: "secret-id",
			SecretKey: "secret-key",
		},
		signer: awsv4.NewSigner(),
	}
	if err := p.signRequest(context.Background(), req, emptyPayloadHash, time.Date(2026, 6, 27, 1, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}

	auth := req.Header.Get("Authorization")
	if !strings.Contains(auth, "Credential=secret-id/20260627/example-region/s3/aws4_request") {
		t.Fatalf("unexpected auth header: %s", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("unexpected signed headers: %s", auth)
	}
}

func TestCleanupSpoolDirRemovesOnlyProxyTempFiles(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "harbor-cos-s3-proxy-stale")
	keep := filepath.Join(dir, "other-file")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keep, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}

	removed, err := cleanupSpoolDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed %d files, want 1", removed)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("expected stale temp file to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("expected non-proxy file to remain: %v", err)
	}
}

func TestCleanupSpoolDirIgnoresMissingDirectory(t *testing.T) {
	removed, err := cleanupSpoolDir(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatalf("removed %d files, want 0", removed)
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
