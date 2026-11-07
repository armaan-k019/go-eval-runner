# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
# Optional CA bundle for builds behind a TLS-intercepting proxy:
#   docker build --secret id=ca_bundle,src=/path/ca.crt ...
RUN --mount=type=secret,id=ca_bundle,required=false \
    if [ -f /run/secrets/ca_bundle ]; then export SSL_CERT_FILE=/run/secrets/ca_bundle; fi; \
    go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
RUN adduser -D -u 10001 eval
COPY --from=build /out/ /usr/local/bin/
USER eval
EXPOSE 7070 9090 9091
ENTRYPOINT ["coordinator"]
