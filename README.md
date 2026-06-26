# Harbor COS S3 Proxy

Small local proxy for Harbor/Distribution versions that send path-style S3
requests to Tencent Cloud COS buckets that only accept virtual-hosted-style
requests.

It accepts requests like:

```text
http://harbor-cos-s3-proxy:19000/your-cos-bucket-appid/docker/registry/v2/...
```

and forwards them to COS as:

```text
https://your-cos-bucket-appid.cos.your-cos-region.myqcloud.com/docker/registry/v2/...
```

The proxy removes Harbor's incoming S3 signature and signs the outgoing COS
request with `AWS4-HMAC-SHA256`.

## Build

```bash
go build -o harbor-cos-s3-proxy .
```

For your x86_64 Ubuntu Harbor host from macOS:

```bash
GOOS=linux GOARCH=amd64 go build -o dist/harbor-cos-s3-proxy-linux-amd64 .
```

## Run

```bash
export LISTEN_ADDR=0.0.0.0:19000
export COS_REGION=your-cos-region
export COS_BUCKET=your-cos-bucket-appid
export COS_SECRET_ID='your-secret-id'
export COS_SECRET_KEY='your-secret-key'

./harbor-cos-s3-proxy
```

Optional:

```bash
export COS_ENDPOINT=https://your-cos-bucket-appid.cos.your-cos-region.myqcloud.com
export COS_SESSION_TOKEN='temporary-token-if-used'
export SPOOL_DIR=/var/lib/harbor-cos-s3-proxy/spool
```

The proxy spools request bodies to disk before forwarding them, because S3 V4
signing needs a payload hash. Put `SPOOL_DIR` on a filesystem with enough free
space for Harbor upload parts.

## Docker Compose

Create the runtime env file:

```bash
cp cos-s3-proxy.env.example cos-s3-proxy.env
vim cos-s3-proxy.env
```

Set the Harbor Docker network name if it is not `harbor_harbor`:

```bash
docker network ls | grep harbor
export HARBOR_NETWORK=harbor_harbor
```

Start the proxy:

```bash
docker compose build --no-cache
docker compose up -d
docker compose ps
```

The Compose file pins the service to `linux/amd64`, and the runtime image is
`ubuntu:24.04`.

## Harbor Configuration

Use the proxy as a local path-style S3 endpoint. The proxy converts it to COS
virtual-hosted-style.

Because the proxy container joins Harbor's Docker network, Harbor can reach it
by the Compose service name.

```yaml
storage_service:
  s3:
    accesskey: dummy
    secretkey: dummy
    region: your-cos-region
    regionendpoint: http://harbor-cos-s3-proxy:19000
    bucket: your-cos-bucket-appid
    secure: false
    v4auth: true
    chunksize: 5242880
    rootdirectory: /
    forcepathstyle: true
  redirect:
    disable: true
```

Then regenerate Harbor config and restart:

```bash
cd /path/to/harbor
./prepare
docker compose down
docker compose up -d
```

Confirm the registry container sees the proxy endpoint:

```bash
docker exec registry cat /etc/registry/config.yml | sed -n '/storage:/,/http:/p'
```
