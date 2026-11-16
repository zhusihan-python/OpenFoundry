#
# OpenFoundry P1 acceptance — shared service builder.
#
# One Dockerfile for dataset-versioning / pipeline-build /
# connector-management. Build context MUST be the repo root; the source
# tree is bind-mounted so source churn never bloats the builder image,
# and all services share the BuildKit module/build caches
# (`of-go-mod`, `of-go-build`) with the earlier file-IO environment.
#
# build args:
#   SERVICE_NAME   services/<name> directory (required)
#   VERSION        baked into the binary (default dev)
#
# If services/${SERVICE_NAME}/config.yaml exists it is installed into
# the runtime image at /etc/openfoundry/config.yaml (pipeline-build
# needs it).
#
ARG GO_VERSION=1.26.3
ARG ALPINE_VERSION=3.22

############################################################
# Stage 1 — modules: pre-warmed Go module graph.
############################################################
FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS modules

ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0 \
    GOFLAGS="-mod=readonly -buildvcs=false" \
    GOPROXY=https://proxy.golang.org,direct \
    GOSUMDB=sum.golang.org

WORKDIR /workspace

COPY --link go.mod go.sum ./
RUN --mount=type=cache,id=of-go-mod,target=/go/pkg/mod,sharing=locked \
    go mod download -x

############################################################
# Stage 2 — builder: compiles the service binary.
############################################################
FROM modules AS builder

ARG SERVICE_NAME
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=arm64

RUN --mount=type=bind,source=.,target=/workspace,rw \
    --mount=type=cache,id=of-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=of-go-build,target=/root/.cache/go-build,sharing=locked \
    set -eux; \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false \
            -ldflags "-s -w -buildid= -extldflags=-static -X main.version=${VERSION}" \
            -tags "osusergo,netgo,static_build" \
            -o /out/service \
            ./services/${SERVICE_NAME}/cmd/${SERVICE_NAME}; \
    mkdir -p /out/etc/openfoundry; \
    if [ -f "services/${SERVICE_NAME}/config.yaml" ]; then \
        install -D -m 0444 "services/${SERVICE_NAME}/config.yaml" /out/etc/openfoundry/config.yaml; \
    fi

############################################################
# Stage 3 — runtime: distroless static, nonroot.
############################################################
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

ARG SERVICE_NAME
LABEL org.opencontainers.image.title="openfoundry-${SERVICE_NAME}" \
      org.opencontainers.image.description="OpenFoundry ${SERVICE_NAME} (P1 acceptance build)" \
      org.opencontainers.image.vendor="OpenFoundry" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /out/service /usr/local/bin/service
COPY --from=builder /out/etc/openfoundry /etc/openfoundry

USER nonroot:nonroot
WORKDIR /home/nonroot
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/service"]
