package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
)

func upload_watcher() {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: BOOTSTRAP_SERVERS,
		GroupID: "FILTER_WORKER",
		Topic:   "DATABASES_UPLOAD_NOTIFICATIONS",
	})

	data_resquests_producer := &kafka.Writer{
		Addr: kafka.TCP(BOOTSTRAP_SERVERS...),
	}

	for {
		msg, _ := consumer.FetchMessage(context.Background())

		var upload map[string]interface{}
		json.Unmarshal(msg.Value, &upload)

		database_identifier := upload["DATABASE_IDENTIFIER"].(string)
		rows := uint32(upload["ROWS"].(float64))

		log.Printf("Received an upload with %d rows from %s\n", rows, database_identifier)

		community := get_community_of_database(database_identifier)

		if community == -1 {
			log.Printf("Unknow database with database identifer %s. Ignoring\n", database_identifier)
			continue // no database with given database_identifier. ignore
		}

		data_request, _ := json.Marshal(map[string]interface{}{
			"filter_worker_id":    FILTER_WORKER_ID,
			"database_identifier": database_identifier,
			"rows":                rows,
		})
		data_resquests_producer.WriteMessages(
			context.Background(),
			kafka.Message{
				Topic: "FILTER_WORKERS_DATA_REQUESTS",
				Value: data_request,
			},
		)

		log.Printf("Sent data request\n")

		Mappings_mtx.Lock()
		filters_to_wait_for := len(Mappings)
		Waiting_for_filters = true
		// FIXME if filter mains perform a Done between these two instructions the program will crash
		Filters_wait_group.Add(filters_to_wait_for)

		for filter_id, filter := range Mappings {
			belongs_to_communities := false
			for _, filter_community_id := range filter.Communities {
				if filter_community_id == community {
					belongs_to_communities = true
					break
				}
			}

			if belongs_to_communities {
				log.Printf("Filter %d will execute\n", filter_id)
			} else {
				log.Printf("Filter %d will not execute\n", filter_id)
			}

			filter.Upload_notifications_chan <- UploadToFilter{
				Database_identifier:    database_identifier,
				Rows:                   rows,
				Belongs_to_communities: belongs_to_communities,
			}
		}

		Mappings_mtx.Unlock()

		log.Printf("Will wait for %d filters\n", filters_to_wait_for)

		Filters_wait_group.Wait()
		// FIXME if filter mains perform a Done between these two instructions the program will crash
		Waiting_for_filters = false

		log.Printf("All filters parsed the uploaded data\n")

		consumer.CommitMessages(context.Background(), msg)
	}
}
