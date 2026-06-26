package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestTargetURLForMultipartUpload(t *testing.T) {
	p := proxyServer{
		cfg: config{
			Bucket:   "example-bucket-1234567890",
			Endpoint: "https://example-bucket-1234567890.cos.example-region.myqcloud.com",
		},
	}
	req := &http.Request{URL: mustURL(t, "http://127.0.0.1:19000/example-bucket-1234567890/example-bucket-1234567890/docker/registry/v2/repositories/repo/_uploads/upload-id/data?uploads=")}
	got, err := p.targetURL(req)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://example-bucket-1234567890.cos.example-region.myqcloud.com/example-bucket-1234567890/docker/registry/v2/repositories/repo/_uploads/upload-id/data?uploads="
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

func TestHandleMultipartUploadInitiate(t *testing.T) {
	var seenMethod, seenPath, seenQuery, seenAuth, seenHash, seenStorageClass string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenMethod = r.Method
		seenPath = r.URL.Path
		seenQuery = r.URL.RawQuery
		seenAuth = r.Header.Get("Authorization")
		seenHash = r.Header.Get("X-Amz-Content-Sha256")
		seenStorageClass = r.Header.Get("X-Amz-Storage-Class")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if len(body) != 0 {
			t.Errorf("body length = %d, want 0", len(body))
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>`))
	}))
	defer upstream.Close()

	p := proxyServer{
		cfg: config{
			Region:    "example-region",
			Bucket:    "example-bucket-1234567890",
			Endpoint:  upstream.URL,
			AccessKey: "secret-id",
			SecretKey: "secret-key",
			SpoolDir:  t.TempDir(),
		},
		client: upstream.Client(),
		signer: awsv4.NewSigner(),
	}

	req := httptest.NewRequest(http.MethodPost, "http://proxy/example-bucket-1234567890/root/docker/registry/v2/blobs/data?uploads=", http.NoBody)
	req.Header.Set("X-Amz-Storage-Class", "STANDARD")
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if seenMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", seenMethod)
	}
	if seenPath != "/root/docker/registry/v2/blobs/data" {
		t.Fatalf("path = %q", seenPath)
	}
	if seenQuery != "uploads=" {
		t.Fatalf("query = %q", seenQuery)
	}
	if !strings.Contains(seenAuth, "Credential=secret-id/") {
		t.Fatalf("missing signed authorization: %s", seenAuth)
	}
	if seenHash != emptyPayloadHash {
		t.Fatalf("payload hash = %q", seenHash)
	}
	if seenStorageClass != "" {
		t.Fatalf("storage class header was forwarded: %q", seenStorageClass)
	}
}

func TestHandleCompleteMultipartUploadTracksUploadDataKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.RawQuery != "uploadId=upload-id" {
			t.Errorf("query = %q", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<CompleteMultipartUploadResult></CompleteMultipartUploadResult>`))
	}))
	defer upstream.Close()

	p := testProxy(upstream)
	req := httptest.NewRequest(http.MethodPost, "http://proxy/example-bucket-1234567890/root/docker/registry/v2/repositories/repo/_uploads/upload-id/data?uploadId=upload-id", strings.NewReader(`<CompleteMultipartUpload/>`))
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	key := "root/docker/registry/v2/repositories/repo/_uploads/upload-id/data"
	if !p.uploadDataKeys.has(key, time.Now()) {
		t.Fatalf("upload data key was not tracked: %s", key)
	}
}

func TestHandleRetriesEmptyCompletedUploadDataList(t *testing.T) {
	key := "root/docker/registry/v2/repositories/repo/_uploads/upload-id/data"
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/" {
			t.Errorf("path = %q, want /", r.URL.Path)
		}
		if got := r.URL.Query().Get("prefix"); got != key {
			t.Errorf("prefix = %q, want %q", got, key)
		}
		w.Header().Set("Content-Type", "application/xml")
		if calls < 3 {
			_, _ = w.Write([]byte(`<ListBucketResult><Name>example-bucket-1234567890</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
			return
		}
		_, _ = w.Write([]byte(`<ListBucketResult><Name>example-bucket-1234567890</Name><Contents><Key>` + key + `</Key></Contents><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer upstream.Close()

	p := testProxy(upstream)
	p.uploadDataKeys.mark(key, time.Now())
	req := httptest.NewRequest(http.MethodGet, "http://proxy/example-bucket-1234567890?max-keys=1&prefix="+url.QueryEscape(key), http.NoBody)
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if calls != 3 {
		t.Fatalf("upstream calls = %d, want 3", calls)
	}
	if !strings.Contains(rec.Body.String(), "<Contents>") {
		t.Fatalf("response did not use visible retry body: %s", rec.Body.String())
	}
}

func TestHandleNormalizesTrackedUploadDataListTruncation(t *testing.T) {
	key := "root/docker/registry/v2/repositories/repo/_uploads/upload-id/data"
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>example-bucket-1234567890</Name><Contents><Key>` + key + `</Key></Contents><NextMarker>` + key + `</NextMarker><IsTruncated>true</IsTruncated></ListBucketResult>`))
	}))
	defer upstream.Close()

	p := testProxy(upstream)
	p.uploadDataKeys.mark(key, time.Now())
	req := httptest.NewRequest(http.MethodGet, "http://proxy/example-bucket-1234567890?max-keys=1&prefix="+url.QueryEscape(key), http.NoBody)
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls)
	}
	if !strings.Contains(rec.Body.String(), "<IsTruncated>false</IsTruncated>") {
		t.Fatalf("response did not normalize truncation: %s", rec.Body.String())
	}
}

func TestHandleDoesNotRetryUploadDataListPagination(t *testing.T) {
	key := "root/docker/registry/v2/repositories/repo/_uploads/upload-id/data"
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>example-bucket-1234567890</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer upstream.Close()

	p := testProxy(upstream)
	p.uploadDataKeys.mark(key, time.Now())
	req := httptest.NewRequest(http.MethodGet, "http://proxy/example-bucket-1234567890?marker="+url.QueryEscape(key)+"&prefix="+url.QueryEscape(key), http.NoBody)
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls)
	}
}

func TestHandleDoesNotRetryUntrackedUploadDataList(t *testing.T) {
	key := "root/docker/registry/v2/repositories/repo/_uploads/upload-id/data"
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>example-bucket-1234567890</Name><IsTruncated>false</IsTruncated></ListBucketResult>`))
	}))
	defer upstream.Close()

	p := testProxy(upstream)
	req := httptest.NewRequest(http.MethodGet, "http://proxy/example-bucket-1234567890?max-keys=1&prefix="+url.QueryEscape(key), http.NoBody)
	rec := httptest.NewRecorder()
	p.handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if calls != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls)
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

func TestEnsureSpoolDirWritable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spool")
	if err := ensureSpoolDirWritable(dir); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("spool dir has %d entries, want 0", len(entries))
	}
}

func testProxy(upstream *httptest.Server) proxyServer {
	return proxyServer{
		cfg: config{
			Region:    "example-region",
			Bucket:    "example-bucket-1234567890",
			Endpoint:  upstream.URL,
			AccessKey: "secret-id",
			SecretKey: "secret-key",
			SpoolDir:  os.TempDir(),
			ListRetry: listRetryConfig{
				Attempts:     3,
				InitialDelay: time.Millisecond,
				MaxDelay:     time.Millisecond,
			},
			CacheTTL: time.Minute,
		},
		client:         upstream.Client(),
		signer:         awsv4.NewSigner(),
		uploadDataKeys: newObjectCache(time.Minute),
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
