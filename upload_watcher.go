package main

import (
	"./filters"

	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

type Database struct {
	id                int
	community         int
	name              string
	unique_identifier string
}

func upload_watcher(filter_worker_id int) {
	BOOTSTRAP_SERVERS := os.Getenv("BOOTSTRAP_SERVERS")
	KSQLDB_HOST := os.Getenv("KSQLDB_HOST")
	KSQLDB_PORT := os.Getenv("KSQLDB_PORT")

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(BOOTSTRAP_SERVERS, ","),
		GroupID: "FILTER_WORKER",
		Topic:   "DATABASES_UPLOAD_NOTIFICATIONS",
	})

	data_resquests_producer := &kafka.Writer{
		Addr: kafka.TCP(strings.Split(BOOTSTRAP_SERVERS, ",")...),
	}

	for {
		msg, _ := consumer.FetchMessage(context.Background())

		var upload filters.Upload
		json.Unmarshal(msg.Value, &upload)

		log.Printf("Received an upload with %d rows from %s\n", upload.Rows, upload.Database_identifier)

		resp, _ := http.Get(fmt.Sprintf(
			"%s:%s/databases/?unique_identifier=%s", KSQLDB_HOST, KSQLDB_PORT, upload.Database_identifier,
		))
		body, _ := ioutil.ReadAll(resp.Body)
		var database []Database
		json.Unmarshal(body, &database)

		if len(database) == 0 {
			log.Printf("Unknow database with database identifer %s. Ignoring\n", upload.Database_identifier)
			continue // no database with given database_identifier. ignore
		}

		data_request, _ := json.Marshal(map[string]interface{}{
			"filter_worker_id":    filter_worker_id,
			"database_identifier": upload.Database_identifier,
			"rows":                upload.Rows,
		})
		data_resquests_producer.WriteMessages(
			context.Background(),
			kafka.Message{
				Topic: "FILTER_WORKERS_DATA_REQUESTS",
				Value: data_request,
			},
		)

		log.Printf("Sent data request\n")

		filters.Mappings_mtx.Lock()
		filters_to_wait_for := len(filters.Mappings)
		filters.Filters_wait_group.Add(filters_to_wait_for)

		for filter_id, filter := range filters.Mappings {
			belongs_to_communities := false
			for _, filter_community_id := range filter.Communities {
				if filter_community_id == database[0].community {
					belongs_to_communities = true
					break
				}
			}

			if belongs_to_communities {
				log.Printf("Filter %d will execute\n", filter_id)
			} else {
				log.Printf("Filter %d will not execute\n", filter_id)
			}

			filter.Upload_notifications_chan <- filters.UploadToFilter{
				Database_identifier:    upload.Database_identifier,
				Rows:                   upload.Rows,
				Belongs_to_communities: belongs_to_communities,
			}
		}

		filters.Mappings_mtx.Unlock()

		log.Printf("Will wait for %d filters\n", filters_to_wait_for)

		filters.Filters_wait_group.Wait()

		log.Printf("All filters parsed the uploaded data\n")
	}
}
