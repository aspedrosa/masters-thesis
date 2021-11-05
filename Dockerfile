FROM golang:1.17-alpine

WORKDIR /app

COPY *.go go.sum go.mod filter_worker_data_topics_schema.json /app/

RUN go get . && go build admin_portal.go filter_entities.go filters.go globals.go ksql.go main.go structs.go topics.go upload_watcher.go

CMD filter-worker
