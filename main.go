package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type config struct {
	ListenAddr   string
	Region       string
	Bucket       string
	AccessKey    string
	SecretKey    string
	SessionToken string
	Endpoint     string
	SpoolDir     string
	ShutdownWait time.Duration
	ListRetry    listRetryConfig
	CacheTTL     time.Duration
}

type listRetryConfig struct {
	Attempts     int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

type proxyServer struct {
	cfg            config
	client         *http.Client
	signer         *awsv4.Signer
	uploadDataKeys *objectCache
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	if err := ensureSpoolDirWritable(cfg.SpoolDir); err != nil {
		log.Fatalf("spool dir is not writable: %v", err)
	}
	if removed, err := cleanupSpoolDir(cfg.SpoolDir); err != nil {
		log.Printf("spool cleanup finished with errors, removed=%d: %v", removed, err)
	} else if removed > 0 {
		log.Printf("spool cleanup removed %d stale temp file(s)", removed)
	}

	srv := &proxyServer{
		cfg: cfg,
		client: &http.Client{
			Timeout: 0,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          200,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   15 * time.Second,
				ExpectContinueTimeout: 5 * time.Second,
				DisableCompression:    true,
			},
		},
		signer:         awsv4.NewSigner(),
		uploadDataKeys: newObjectCache(cfg.CacheTTL),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/", srv.handle)

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: mux,
	}

	log.Printf("listening on %s, forwarding bucket %q to %s", cfg.ListenAddr, cfg.Bucket, cfg.Endpoint)
	if err := runServer(httpServer, cfg.ShutdownWait); err != nil {
		log.Fatal(err)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		ListenAddr: getenv("LISTEN_ADDR", "127.0.0.1:19000"),
		Region:     os.Getenv("COS_REGION"),
		Bucket:     os.Getenv("COS_BUCKET"),
		AccessKey:  os.Getenv("COS_SECRET_ID"),
		SecretKey:  os.Getenv("COS_SECRET_KEY"),
		SpoolDir:   getenv("SPOOL_DIR", os.TempDir()),
	}
	cfg.SessionToken = os.Getenv("COS_SESSION_TOKEN")

	if cfg.Bucket == "" {
		return cfg, errors.New("COS_BUCKET is required")
	}
	if cfg.AccessKey == "" {
		return cfg, errors.New("COS_SECRET_ID is required")
	}
	if cfg.SecretKey == "" {
		return cfg, errors.New("COS_SECRET_KEY is required")
	}
	if cfg.Region == "" {
		return cfg, errors.New("COS_REGION is required")
	}
	shutdownWait, err := parseDurationEnv("SHUTDOWN_TIMEOUT", 30*time.Second)
	if err != nil {
		return cfg, err
	}
	cfg.ShutdownWait = shutdownWait
	cfg.CacheTTL, err = parseDurationEnv("UPLOAD_DATA_CACHE_TTL", 10*time.Minute)
	if err != nil {
		return cfg, err
	}
	cfg.ListRetry.Attempts, err = parseIntEnv("LIST_RETRY_ATTEMPTS", 5)
	if err != nil {
		return cfg, err
	}
	cfg.ListRetry.InitialDelay, err = parseDurationEnv("LIST_RETRY_INITIAL_DELAY", 100*time.Millisecond)
	if err != nil {
		return cfg, err
	}
	cfg.ListRetry.MaxDelay, err = parseDurationEnv("LIST_RETRY_MAX_DELAY", 2*time.Second)
	if err != nil {
		return cfg, err
	}

	cfg.Endpoint = os.Getenv("COS_ENDPOINT")
	if cfg.Endpoint == "" {
		cfg.Endpoint = fmt.Sprintf("https://%s.cos.%s.myqcloud.com", cfg.Bucket, cfg.Region)
	}
	if !strings.HasPrefix(cfg.Endpoint, "http://") && !strings.HasPrefix(cfg.Endpoint, "https://") {
		return cfg, fmt.Errorf("COS_ENDPOINT must include scheme: %s", cfg.Endpoint)
	}
	cfg.Endpoint = strings.TrimRight(cfg.Endpoint, "/")

	return cfg, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a Go duration such as 30s or 2m: %w", key, err)
	}
	if duration <= 0 {
		return 0, fmt.Errorf("%s must be positive", key)
	}
	return duration, nil
}

func parseIntEnv(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s must be non-negative", key)
	}
	return parsed, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type objectCache struct {
	mu   sync.Mutex
	ttl  time.Duration
	keys map[string]time.Time
}

func newObjectCache(ttl time.Duration) *objectCache {
	return &objectCache{
		ttl:  ttl,
		keys: make(map[string]time.Time),
	}
}

func (c *objectCache) mark(key string, now time.Time) {
	if c == nil || key == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.keys[key] = now.Add(c.ttl)
	c.cleanupLocked(now)
}

func (c *objectCache) has(key string, now time.Time) bool {
	if c == nil || key == "" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	expiresAt, ok := c.keys[key]
	if !ok {
		return false
	}
	if !now.Before(expiresAt) {
		delete(c.keys, key)
		return false
	}
	c.cleanupLocked(now)
	return true
}

func (c *objectCache) cleanupLocked(now time.Time) {
	for key, expiresAt := range c.keys {
		if !now.Before(expiresAt) {
			delete(c.keys, key)
		}
	}
}

func runServer(server *http.Server, shutdownWait time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case sig := <-sigCh:
		log.Printf("received %s, shutting down with %s grace period", sig, shutdownWait)
		ctx, cancel := context.WithTimeout(context.Background(), shutdownWait)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("graceful shutdown failed: %v; forcing close", err)
			if closeErr := server.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
			return err
		}
		err := <-errCh
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

const spoolTempPrefix = "harbor-cos-s3-proxy-"

func cleanupSpoolDir(spoolDir string) (int, error) {
	entries, err := os.ReadDir(spoolDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	var errs []error
	removed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), spoolTempPrefix) {
			continue
		}
		path := filepath.Join(spoolDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			errs = append(errs, fmt.Errorf("stat %s: %w", path, err))
			continue
		}
		if !info.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(path); err != nil {
			errs = append(errs, fmt.Errorf("remove %s: %w", path, err))
			continue
		}
		removed++
	}
	return removed, errors.Join(errs...)
}

func ensureSpoolDirWritable(spoolDir string) error {
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(spoolDir, spoolTempPrefix+"write-check-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	if err := os.Remove(name); err != nil {
		return err
	}
	return nil
}

func (p *proxyServer) handle(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	targetURL, err := p.targetURL(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	body, cleanup, payloadHash, contentLength, err := p.prepareBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		log.Printf("%s %s body prepare error after %s: %v", r.Method, r.URL.RequestURI(), time.Since(start), err)
		return
	}
	defer cleanup()

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outReq.ContentLength = contentLength
	outReq.Host = outReq.URL.Host
	copyForwardHeaders(outReq.Header, r.Header)
	if r.ContentLength == 0 {
		outReq.ContentLength = 0
	}

	if err := p.signRequest(r.Context(), outReq, payloadHash, time.Now().UTC()); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		log.Printf("%s %s -> %s sign error after %s: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), time.Since(start), err)
		return
	}

	resp, err := p.client.Do(outReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		log.Printf("%s %s -> %s content_length=%d payload_hash=%s network error after %s: %s", r.Method, r.URL.RequestURI(), targetURL.Redacted(), outReq.ContentLength, payloadHash, time.Since(start), describeNetworkError(err))
		return
	}
	replayBody, err := p.retryEmptyCompletedUploadList(r.Context(), r, targetURL, resp, start)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		log.Printf("%s %s -> %s response handling error after %s: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), time.Since(start), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusBadRequest {
		p.observeSuccessfulRequest(r, targetURL)
	}

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if resp.StatusCode >= http.StatusBadRequest {
		body := replayBody
		if body == nil {
			body, err = io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("%s %s -> %s %d response read error after %s: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), resp.StatusCode, time.Since(start), err)
				return
			}
		}
		if len(body) > 0 {
			log.Printf("%s %s -> %s %d %s upstream error: %s", r.Method, r.URL.RequestURI(), targetURL.Redacted(), resp.StatusCode, time.Since(start), trimLogBody(body, 2048))
		} else {
			log.Printf("%s %s -> %s %d %s upstream error with empty body", r.Method, r.URL.RequestURI(), targetURL.Redacted(), resp.StatusCode, time.Since(start))
		}
		_, _ = w.Write(body)
		return
	}

	if replayBody != nil {
		_, _ = w.Write(replayBody)
	} else {
		if _, err := io.Copy(w, resp.Body); err != nil {
			log.Printf("%s %s -> %s response copy error: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), err)
			return
		}
	}

	log.Printf("%s %s -> %s %d %s", r.Method, r.URL.RequestURI(), targetURL.Redacted(), resp.StatusCode, time.Since(start))
}

func (p *proxyServer) observeSuccessfulRequest(r *http.Request, targetURL *url.URL) {
	if isCompleteMultipartUploadRequest(r.Method, targetURL) {
		key := objectKeyFromTargetURL(targetURL)
		p.uploadDataKeys.mark(key, time.Now())
		log.Printf("tracking completed upload data object for list retry: %s", key)
	}
}

func (p *proxyServer) retryEmptyCompletedUploadList(ctx context.Context, original *http.Request, targetURL *url.URL, resp *http.Response, start time.Time) ([]byte, error) {
	prefix, ok := p.retryableUploadDataList(original.Method, targetURL)
	if !ok {
		return nil, nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !p.uploadDataKeys.has(prefix, time.Now()) {
		return body, nil
	}
	if hasExactListKey(body, prefix) {
		normalized := normalizeSingleObjectList(body)
		if string(normalized) != string(body) {
			log.Printf("%s %s -> %s normalized tracked upload list truncation after %s", original.Method, original.URL.RequestURI(), targetURL.Redacted(), time.Since(start))
		}
		return normalized, nil
	}
	if hasListContents(body) || p.cfg.ListRetry.Attempts == 0 {
		return body, nil
	}

	delay := p.cfg.ListRetry.InitialDelay
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	maxDelay := p.cfg.ListRetry.MaxDelay
	if maxDelay <= 0 {
		maxDelay = delay
	}

	currentBody := body
	currentResp := resp
	for attempt := 1; attempt <= p.cfg.ListRetry.Attempts; attempt++ {
		if err := sleepWithContext(ctx, delay); err != nil {
			return currentBody, nil
		}

		retryResp, retryBody, err := p.doSignedEmptyRequest(ctx, original, targetURL)
		if err != nil {
			log.Printf("%s %s -> %s empty upload list retry %d/%d failed after %s: %s", original.Method, original.URL.RequestURI(), targetURL.Redacted(), attempt, p.cfg.ListRetry.Attempts, time.Since(start), describeNetworkError(err))
		} else {
			currentResp = retryResp
			currentBody = retryBody
			if retryResp.StatusCode != http.StatusOK || hasListContents(retryBody) {
				if retryResp.StatusCode == http.StatusOK && hasExactListKey(retryBody, prefix) {
					currentBody = normalizeSingleObjectList(retryBody)
				}
				*resp = *currentResp
				log.Printf("%s %s -> %s empty upload list became visible on retry %d/%d after %s", original.Method, original.URL.RequestURI(), targetURL.Redacted(), attempt, p.cfg.ListRetry.Attempts, time.Since(start))
				return currentBody, nil
			}
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	*resp = *currentResp
	log.Printf("%s %s -> %s empty upload list still empty after %d retries and %s", original.Method, original.URL.RequestURI(), targetURL.Redacted(), p.cfg.ListRetry.Attempts, time.Since(start))
	return currentBody, nil
}

func (p *proxyServer) doSignedEmptyRequest(ctx context.Context, original *http.Request, targetURL *url.URL) (*http.Response, []byte, error) {
	outReq, err := http.NewRequestWithContext(ctx, original.Method, targetURL.String(), http.NoBody)
	if err != nil {
		return nil, nil, err
	}
	outReq.ContentLength = 0
	outReq.Host = outReq.URL.Host
	copyForwardHeaders(outReq.Header, original.Header)
	if err := p.signRequest(ctx, outReq, emptyPayloadHash, time.Now().UTC()); err != nil {
		return nil, nil, err
	}
	resp, err := p.client.Do(outReq)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	return resp, body, nil
}

func isCompleteMultipartUploadRequest(method string, targetURL *url.URL) bool {
	return method == http.MethodPost && targetURL.Query().Get("uploadId") != "" && isUploadDataKey(objectKeyFromTargetURL(targetURL))
}

func (p *proxyServer) retryableUploadDataList(method string, targetURL *url.URL) (string, bool) {
	if method != http.MethodGet || targetURL.Path != "/" {
		return "", false
	}
	if targetURL.Query().Get("marker") != "" {
		return "", false
	}
	prefix := targetURL.Query().Get("prefix")
	if !isUploadDataKey(prefix) {
		return "", false
	}
	return prefix, true
}

func objectKeyFromTargetURL(targetURL *url.URL) string {
	return strings.TrimPrefix(targetURL.Path, "/")
}

func isUploadDataKey(key string) bool {
	return strings.Contains(key, "/_uploads/") && strings.HasSuffix(key, "/data")
}

func hasListContents(body []byte) bool {
	return strings.Contains(string(body), "<Contents>") && strings.Contains(string(body), "<Key>")
}

type listBucketResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

func hasExactListKey(body []byte, key string) bool {
	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return false
	}
	for _, content := range result.Contents {
		if content.Key == key {
			return true
		}
	}
	return false
}

var isTruncatedTruePattern = regexp.MustCompile(`(?is)<IsTruncated>\s*true\s*</IsTruncated>`)

func normalizeSingleObjectList(body []byte) []byte {
	return isTruncatedTruePattern.ReplaceAll(body, []byte("<IsTruncated>false</IsTruncated>"))
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (p *proxyServer) targetURL(r *http.Request) (*url.URL, error) {
	base, err := url.Parse(p.cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	stripped, err := stripBucketPrefix(r.URL.Path, p.cfg.Bucket)
	if err != nil {
		return nil, err
	}

	u := *base
	u.Path = stripped
	u.RawPath = ""
	u.RawQuery = r.URL.RawQuery
	return &u, nil
}

func stripBucketPrefix(requestPath, bucket string) (string, error) {
	prefix := "/" + bucket
	switch {
	case requestPath == prefix:
		return "/", nil
	case strings.HasPrefix(requestPath, prefix+"/"):
		return requestPath[len(prefix):], nil
	default:
		return "", fmt.Errorf("expected path to start with /%s, got %q", bucket, requestPath)
	}
}

func (p *proxyServer) prepareBody(r *http.Request) (io.Reader, func(), string, int64, error) {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return http.NoBody, func() {}, emptyPayloadHash, 0, nil
	}

	tmp, err := os.CreateTemp(p.cfg.SpoolDir, spoolTempPrefix+"*")
	if err != nil {
		return nil, func() {}, "", 0, err
	}

	cleanup := func() {
		name := tmp.Name()
		_ = tmp.Close()
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			log.Printf("failed to remove spool temp file %s: %v", name, err)
		}
	}

	hasher := sha256.New()
	size, err := copyAndHash(tmp, r.Body, hasher)
	_ = r.Body.Close()
	if err != nil {
		cleanup()
		return nil, func() {}, "", 0, err
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		cleanup()
		return nil, func() {}, "", 0, err
	}

	return tmp, cleanup, hex.EncodeToString(hasher.Sum(nil)), size, nil
}

func copyAndHash(dst io.Writer, src io.Reader, h hash.Hash) (int64, error) {
	return io.Copy(io.MultiWriter(dst, h), src)
}

func copyForwardHeaders(dst, src http.Header) {
	for key, values := range src {
		if shouldSkipRequestHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func shouldSkipRequestHeader(key string) bool {
	switch strings.ToLower(key) {
	case "authorization", "connection", "content-length", "host", "proxy-authenticate", "proxy-authorization",
		"accept-encoding", "expect", "te", "trailer", "transfer-encoding", "upgrade", "x-amz-content-sha256",
		"x-amz-date", "x-amz-security-token", "x-amz-storage-class", "x-cos-storage-class":
		return true
	default:
		return false
	}
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		if shouldSkipResponseHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func shouldSkipResponseHeader(key string) bool {
	switch strings.ToLower(key) {
	case "connection", "content-length", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

func trimLogBody(body []byte, limit int) string {
	text := strings.TrimSpace(string(body))
	if len(text) <= limit {
		return text
	}
	return text[:limit] + "...<truncated>"
}

func describeNetworkError(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		var netErr net.Error
		if errors.As(urlErr.Err, &netErr) {
			return fmt.Sprintf("%T: %v timeout=%t temporary=%t", urlErr.Err, urlErr.Err, netErr.Timeout(), netErr.Temporary())
		}
		return fmt.Sprintf("%T: %v", urlErr.Err, urlErr.Err)
	}
	return fmt.Sprintf("%T: %v", err, err)
}

const emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func (p *proxyServer) signRequest(ctx context.Context, req *http.Request, payloadHash string, now time.Time) error {
	creds := aws.Credentials{
		AccessKeyID:     p.cfg.AccessKey,
		SecretAccessKey: p.cfg.SecretKey,
		SessionToken:    p.cfg.SessionToken,
		Source:          "harbor-cos-s3-proxy-env",
	}
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	return p.signer.SignHTTP(ctx, creds, req, payloadHash, "s3", p.cfg.Region, now)
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if dir := os.Getenv("SPOOL_DIR"); dir != "" {
		if abs, err := filepath.Abs(dir); err == nil {
			_ = os.Setenv("SPOOL_DIR", abs)
		}
	}
}
