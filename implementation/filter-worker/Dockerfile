FROM golang:1.17-alpine AS build

WORKDIR /app

COPY *.go go.sum go.mod /app/

RUN go get . && go build admin_portal.go filter_entities.go filters.go globals.go ksql.go main.go structs.go topics.go upload_watcher.go

FROM alpine:3.14

WORKDIR /app

COPY --from=build /go/bin/filter-worker /usr/local/bin

COPY filter_worker_data_topics_avro_schema.json /app

CMD filter-worker
