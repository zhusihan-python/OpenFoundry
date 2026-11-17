# schedules-tick — the single-shot scheduling tick CLI
# (libs/event-scheduler/cmd/schedules-tick), WP3.2 / T8.
#
# NOTE: no `# syntax=` directive — pulling the dockerfile frontend
# image requires auth.docker.io which is unreachable from this host's
# daemon; the BuildKit built-in frontend supports every feature used
# below (RUN --mount, COPY --link).
#
# Same multi-stage / distroless / cache strategy as the service
# Dockerfiles (see services/pipeline-build-service/Dockerfile); build
# context MUST be the monorepo root:
#
#   docker build -f libs/event-scheduler/Dockerfile \
#       --build-arg VERSION=$(git describe --tags --always) \
#       -t openfoundry/schedules-tick:$VERSION .

ARG GO_VERSION=1.26.3
ARG ALPINE_VERSION=3.22
ARG DISTROLESS_IMAGE=gcr.io/distroless/static-debian12:nonroot

############################################################
# Stage 1 — modules: pre-warmed Go module graph.
############################################################
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS modules

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
# Stage 2 — builder: compiles the schedules-tick binary.
############################################################
FROM modules AS builder

ARG VERSION=dev
# Redeclared WITHOUT defaults so BuildKit's automatic platform ARGs
# populate them (official Dockerfile reference, automatic-platform-args);
# callers that need a pinned target pass --build-arg explicitly (the
# ofwp32 runner pins linux/arm64 for the kind node).
ARG TARGETOS
ARG TARGETARCH

RUN --mount=type=bind,source=.,target=/workspace,rw \
    --mount=type=cache,id=of-go-mod,target=/go/pkg/mod,sharing=locked \
    --mount=type=cache,id=of-go-build,target=/root/.cache/go-build,sharing=locked \
    set -eux; \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false -p 2 \
            -ldflags "-s -w -buildid= -extldflags=-static -X main.version=${VERSION}" \
            -tags "osusergo,netgo,static_build" \
            -o /out/schedules-tick \
            ./libs/event-scheduler/cmd/schedules-tick

############################################################
# Stage 3 — runtime: minimal, immutable, nonroot.
############################################################
FROM ${DISTROLESS_IMAGE} AS runtime

ARG VERSION=dev
ARG VCS_REF=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="openfoundry-schedules-tick" \
      org.opencontainers.image.description="OpenFoundry scheduling tick CLI (direct + pipeline_outbox_v1 dispatch modes)" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.created="${BUILD_DATE}" \
      org.opencontainers.image.source="https://github.com/unnamedlab/OpenFoundry" \
      org.opencontainers.image.vendor="OpenFoundry" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="gcr.io/distroless/static-debian12:nonroot"

COPY --from=builder /out/schedules-tick /usr/local/bin/schedules-tick

USER nonroot:nonroot
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/schedules-tick"]
