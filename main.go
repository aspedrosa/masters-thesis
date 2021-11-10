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
		Brokers: BOOTSTRAP_SERVERS,
		Topic:   "FILTER_WORKERS_MANAGEMENT",
	})
	consumer.SetOffset(kafka.LastOffset)

	log.Println("Listening to filter management messages")

	for {
		message, _ := consumer.FetchMessage(context.Background())

		var message_value map[string]interface{}
		json.Unmarshal(message.Value, &message_value)

		var filter Filter
		filter.Id = int(message_value["filter_id"].(float64))

		if message_value["action"].(string) == "ACTIVE" {
			json.Unmarshal(message.Value, &filter)

			log.Printf("Launching filter with id %d\n", filter.Id)

			Launch_filter(filter, true)
		} else if message_value["action"].(string) == "STOPPED" {
			log.Printf("Stopping filter with id %d\n", filter.Id)

			Stop_filter(filter.Id)
		} else if message_value["action"].(string) == "EDIT" {
			json.Unmarshal(message.Value, &filter)

			log.Printf("Editting filter with id %d\n", filter.Id)

			Edit_filter(filter)
		} else {
			log.Printf("Invalid action %s\n", message_value["action"].(string))
		}
	}
}

func launch_entities() {
	log.Println("Fetching for active filters")

	for _, filter := range get_active_filters() {
		log.Printf("Launching active filter %d\n", filter.Id)

		go Launch_filter(filter, false)
	}

	go upload_watcher()
}
