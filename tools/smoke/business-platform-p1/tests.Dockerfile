#
# OpenFoundry P1 acceptance — runtime test runner image.
#
# IMPORTANT: no tests run at BUILD time. The image only pre-warms the Go
# module graph; the repo is bind-mounted at `docker compose run` time and
# `run_tests.sh` executes the suites inside the compose network so the
# integration tests can use the runtime Postgres DSNs
# (DATASET_DB_DSN / PIPELINE_DB_DSN / CONNECTOR_DB_DSN).
#
# Build context MUST be the repo root.
#
ARG GO_VERSION=1.26.11
ARG ALPINE_VERSION=3.23

FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:${GO_VERSION}-alpine${ALPINE_VERSION} AS test-runner

ENV GOTOOLCHAIN=local \
    CGO_ENABLED=0 \
    GOFLAGS="-mod=readonly -buildvcs=false" \
    GOPROXY=https://proxy.golang.org,direct \
    GOSUMDB=sum.golang.org

WORKDIR /workspace

COPY --link go.mod go.sum ./
RUN --mount=type=cache,id=of-go-mod,target=/go/pkg/mod,sharing=locked \
    go mod download -x

# Source tree and the runner script arrive via bind mounts; tests are
# executed by `docker compose run` (see compose.yaml.template).
ENTRYPOINT ["sh", "tools/smoke/business-platform-p1/run_tests.sh"]
