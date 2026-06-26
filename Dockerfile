FROM --platform=$BUILDPLATFORM golang:1.23 AS build
ARG TARGETOS=linux
ARG TARGETARCH=amd64
WORKDIR /src
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /out/harbor-cos-s3-proxy .

FROM ubuntu:24.04
RUN sed -i 's@//.*archive.ubuntu.com@//mirrors.ustc.edu.cn@g' /etc/apt/sources.list.d/ubuntu.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system cosproxy \
    && useradd --system --gid cosproxy --home-dir /nonexistent --shell /usr/sbin/nologin cosproxy \
    && mkdir -p /spool \
    && chown cosproxy:cosproxy /spool
USER cosproxy
COPY --from=build /out/harbor-cos-s3-proxy /usr/local/bin/harbor-cos-s3-proxy
ENTRYPOINT ["/usr/local/bin/harbor-cos-s3-proxy"]
