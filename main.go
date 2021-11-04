package main

import (
	"./filters"
	"./globals"
	"./ksql"
	"./shared_structs"

	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
)

func main() {
	globals.Init_global_variables()
	ksql.Init_schema_registry_client()

	create_topics()
	ksql.Init_data_stream()
	launch_entities()

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     globals.BOOTSTRAP_SERVERS,
		Topic:       "FILTER_WORKERS_MANAGEMENT",
		StartOffset: kafka.LastOffset,
	})

	log.Println("Listening to filter management messages")

	for {
		message, _ := consumer.FetchMessage(context.Background())

		var message_value map[string]interface{}
		json.Unmarshal(message.Value, &message_value)

		var filter shared_structs.Filter
		filter.Id = message_value["filter_id"].(int)

		if message_value["action"].(string) == "ACTIVE" {
			json.Unmarshal(message.Value, &filter)

			log.Printf("Launching filter with id %d\n", filter.Id)

			filters.Launch_filter(filter, true)

		} else if message_value["action"].(string) == "STOPPED" {
			log.Printf("Stopping filter with id %d\n", filter.Id)

			filters.Stop_filter(filter.Id)
		} else {
			log.Printf("Invalid action %s\n", message_value["action"].(string))
		}

		// TODO edit filter
	}
}

func launch_entities() {
	log.Println("Fetching for active filters")

	for _, filter := range get_active_filters() {
		log.Printf("Launching active filter %d\n", filter.Id)

		go filters.Launch_filter(filter, false)
	}

	go upload_watcher()
}
