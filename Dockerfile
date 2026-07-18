# syntax=docker/dockerfile:1.7
FROM golang:1.24.5 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -buildid=" -o /out/sentinel ./cmd/manager

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/sentinel /sentinel
USER 65532:65532
ENTRYPOINT ["/sentinel"]
