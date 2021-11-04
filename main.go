package main

import (
	"./filters"
	"./globals"
	"./ksql"
	"./shared_structs"

	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/segmentio/kafka-go"
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

		var message_value shared_structs.ManagementMessage
		json.Unmarshal(message.Value, &message_value)

		var filter shared_structs.Filter
		filter.Id = message_value.FilterId

		if message_value.Action == "ACTIVE" {
			resp, err := http.Get(fmt.Sprintf("%s/filter/%d/", globals.ADMIN_PORTAL_URL, filter.Id))
			if err != nil {
				log.Fatalln(err)
			}

			body, _ := ioutil.ReadAll(resp.Body)

			json.Unmarshal(body, &filter)

			log.Printf("Launching filter with id %d\n", filter.Id)

			filters.Launch_filter(filter, true)

		} else if message_value.Action == "STOPPED" {
			log.Printf("Stopping filter with id %d\n", filter.Id)

			filters.Stop_filter(message_value.FilterId)
		} else {
			log.Printf("Invalid action %s\n", message_value.Action)
		}

		// TODO edit filter
	}
}

func launch_entities() {
	log.Println("Fetching for active filters")

	var active_filters []shared_structs.Filter
	resp, err := http.Get(fmt.Sprintf("%s/filters/?status=ACTIVE", globals.FILTER_WORKER_ID))
	if err != nil {
		log.Fatalln(err)
	}
	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &active_filters)

	for _, filter := range active_filters {
		log.Printf("Launching active filter %d\n", filter.Id)

		go filters.Launch_filter(filter, false)
	}

	go upload_watcher(globals.FILTER_WORKER_ID)
}
