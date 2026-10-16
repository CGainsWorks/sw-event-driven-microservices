# Build image shared by both Go services.
#
# One Dockerfile rather than two near-identical copies: the services differ only in which
# main package is built, so that is a build argument. Two copies would drift, and the drift
# would be in the base image or the build flags — exactly the things that must not differ
# between two services you want to reason about together.
#
# Build context is the repository root, because this is a single Go module and the service
# depends on libs/, proto/gen/ and events/.
#
#   docker build -f services/Dockerfile --build-arg SERVICE=orders .
#
# proto/gen/ is not committed, so the image generates it in its own stage below. That is
# what lets `docker compose up --build` work from a fresh clone with no local buf. The same
# stage is exported to the host by `make proto-docker`.

ARG GO_VERSION=1.26

# Codegen versions. Keep these in step with the pins in the Makefile and in CI.
ARG BUF_VERSION=1.58.0

FROM bufbuild/buf:${BUF_VERSION} AS buf

# --- generated protobuf code ------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS protogen
ARG PROTOC_GEN_GO_VERSION=v1.36.10
ARG PROTOC_GEN_GO_GRPC_VERSION=v1.5.1
COPY --from=buf /usr/local/bin/buf /usr/local/bin/buf
# Plugins before the .proto files, so editing a contract does not reinstall them.
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION} \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}
WORKDIR /src/proto
COPY proto/ ./
RUN rm -rf gen && buf generate

# Just the generated tree, for `docker build --target protogen-out --output ...`.
FROM scratch AS protogen-out
COPY --from=protogen /src/proto/gen /

# --- service binary ---------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS build
ARG SERVICE
WORKDIR /src

# Dependencies first, in their own layer. They change far less often than source, so a
# source-only edit reuses this layer and the build stays under ten seconds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Always the freshly generated code, never whatever happens to be in the build context.
# .dockerignore keeps a local proto/gen/ out of the context so nothing stale survives.
COPY --from=protogen /src/proto/gen ./proto/gen

# Fail loudly rather than producing an image for a service that does not exist.
RUN test -n "${SERVICE}" || (echo "SERVICE build arg is required" && exit 1)

# CGO_ENABLED=0 for a static binary that runs on a distroless base with no libc.
# -trimpath so the binary does not embed the build machine's directory layout.
# The version stamp makes a running container traceable to a commit, which matters the
# first time two replicas disagree about something.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/service \
        ./services/${SERVICE}/cmd/server

# Distroless static: no shell, no package manager, no libc. A smaller attack surface, and
# nothing to exec into if something gets in. The cost is that debugging needs
# `docker cp` or an ephemeral container rather than `docker exec sh`, which is a trade
# worth making for a service that handles payments.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
