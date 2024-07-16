FROM golang:1.19 AS build

ENV CGO_ENABLED=0
ENV GOOS=linux

WORKDIR /build/
COPY . /build/

RUN go build -tags="timetzdata" -ldflags "-w -s" ./cmd/benthos

FROM ghcr.io/redpanda-data/connect

LABEL maintainer="Antoine Girard <antoine.girard@sapk.fr>"
LABEL org.opencontainers.image.source="https://github.com/sapk/benthos-plugin-couchbase"

# replace original benthos binary and configuration
COPY ./config/couchbase.yaml /benthos.yaml
COPY --from=build /build/benthos .

