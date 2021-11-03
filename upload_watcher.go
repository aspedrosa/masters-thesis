package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"os"
	"strings"
)

func upload_watcher(filter_worker_id int) {
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap_servers, ","),
		GroupID: "FILTER_WORKER",
		Topic:   "DATABASES_UPLOAD_NOTIFICATIONS",
	})

	data_resquests_producer := &kafka.Writer{
		Addr: kafka.TCP(strings.Split(bootstrap_servers, ",")...),
	}

	for {
		msg, _ := consumer.FetchMessage(context.Background())

		var upload Upload
		json.Unmarshal(msg.Value, &upload)

		data_request, _ := json.Marshal(map[string]interface{}{
			"filter_worker_id":    filter_worker_id,
			"database_identifier": upload.database_identifier,
			"rows":                upload.rows,
		})
		data_resquests_producer.WriteMessages(
			context.Background(),
			kafka.Message{
				Topic: "FILTER_WORKERS_DATA_REQUESTS",
				Value: data_request,
			},
		)

		// TODO get the community of this database
		community := 1

		broadcast_upload_notification(upload, community)
	}
}
