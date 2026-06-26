package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
}

type proxyServer struct {
	cfg    config
	client *http.Client
	signer *awsv4.Signer
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
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
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				MaxIdleConns:          200,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   15 * time.Second,
				ExpectContinueTimeout: 5 * time.Second,
			},
		},
		signer: awsv4.NewSigner(),
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

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
		return
	}
	defer cleanup()

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	outReq.ContentLength = contentLength
	copyForwardHeaders(outReq.Header, r.Header)
	if r.ContentLength == 0 {
		outReq.ContentLength = 0
	}

	if err := p.signRequest(r.Context(), outReq, payloadHash, time.Now().UTC()); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	resp, err := p.client.Do(outReq)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		log.Printf("%s %s -> %s error after %s: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), time.Since(start), err)
		return
	}
	defer resp.Body.Close()

	copyResponseHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("%s %s -> %s response copy error: %v", r.Method, r.URL.RequestURI(), targetURL.Redacted(), err)
		return
	}

	log.Printf("%s %s -> %s %d %s", r.Method, r.URL.RequestURI(), targetURL.Redacted(), resp.StatusCode, time.Since(start))
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
	case "authorization", "connection", "host", "proxy-authenticate", "proxy-authorization",
		"te", "trailer", "transfer-encoding", "upgrade", "x-amz-content-sha256",
		"x-amz-date", "x-amz-security-token":
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
	case "connection", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
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
