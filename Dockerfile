FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/quorumkv ./cmd/quorumkv

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/quorumkv /usr/local/bin/quorumkv
EXPOSE 8080
ENTRYPOINT ["quorumkv"]
