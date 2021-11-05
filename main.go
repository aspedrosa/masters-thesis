package main

import (
	"github.com/segmentio/kafka-go"

	"context"
	"encoding/json"
	"log"
)

func main() {
	Init_global_variables()
	Init_schema_registry_client()

	create_topics()
	Init_data_stream()
	launch_entities()

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     BOOTSTRAP_SERVERS,
		Topic:       "FILTER_WORKERS_MANAGEMENT",
		StartOffset: kafka.LastOffset,
	})

	log.Println("Listening to filter management messages")

	for {
		message, _ := consumer.FetchMessage(context.Background())

		var message_value map[string]interface{}
		json.Unmarshal(message.Value, &message_value)

		var filter Filter
		filter.id = message_value["filter_id"].(int)

		if message_value["action"].(string) == "ACTIVE" {
			json.Unmarshal(message.Value, &filter)

			log.Printf("Launching filter with id %d\n", filter.id)

			Launch_filter(filter, true)

		} else if message_value["action"].(string) == "STOPPED" {
			log.Printf("Stopping filter with id %d\n", filter.id)

			Stop_filter(filter.id)
		} else {
			log.Printf("Invalid action %s\n", message_value["action"].(string))
		}

		// TODO edit filter
	}
}

func launch_entities() {
	log.Println("Fetching for active filters")

	for _, filter := range get_active_filters() {
		log.Printf("Launching active filter %d\n", filter.id)

		go Launch_filter(filter, true)
	}

	go upload_watcher()
}
