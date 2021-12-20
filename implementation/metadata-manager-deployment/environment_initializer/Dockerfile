FROM golang:1.17-alpine AS build

WORKDIR /app

COPY *.go go.sum go.mod /app/

RUN go get . && go build main.go

FROM alpine:3.14

WORKDIR /app

COPY --from=build /go/bin/environment_initializer /usr/local/bin

CMD environment_initializer
