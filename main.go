package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/segmentio/kafka-go"
	"os"
	"strings"
)

type Filter struct {
	id          int
	selection   []string
	communities []int
	filter      string
}

type ManagementMessage struct {
	FilterId int    `json:"filter_id"`
	Action   string `json:"action"`
}

func main() {
	filter_worker_id := 0 // TODO get this as a program argument OR have generate unique id mechanism

	// kafka related
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")
	admin_portal_backend := os.Getenv("ADMIN_PORTAL_BACKEND")

	create_topics(filter_worker_id, bootstrap_servers)
	init_data_stream(filter_worker_id)

	log.Println("Fetching for active filters")

	var filters []Filter
	{
		resp, err := http.Get(fmt.Sprintf("%s/filter", admin_portal_backend))
		if err != nil {
			log.Fatalln(err)
		}

		body, _ := ioutil.ReadAll(resp.Body)

		json.Unmarshal(body, &filters)
	}

	for _, filter := range filters {
		log.Printf("Launching active pipeline %d\n", filter.id)

		go launch_filter(filter_worker_id, filter, false)
	}

	go upload_watcher(filter_worker_id)

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     strings.Split(bootstrap_servers, ","),
		Topic:       "FILTER_WORKERS_MANAGEMENT",
		StartOffset: kafka.LastOffset,
	})

	log.Println("Listening to filter management messages")

	for {
		message, _ := consumer.FetchMessage(context.Background())

		var message_value ManagementMessage
		json.Unmarshal(message.Value, &message_value)

		var filter Filter
		filter.id = message_value.FilterId

		if message_value.Action == "ACTIVE" {
			resp, err := http.Get(fmt.Sprintf("%s/filter/%d/", admin_portal_backend, filter.id))
			if err != nil {
				log.Fatalln(err)
			}

			body, _ := ioutil.ReadAll(resp.Body)

			json.Unmarshal(body, &filter)

			log.Printf("Launching filter with id %d\n", filter.id)

			launch_filter(filter_worker_id, filter, true)

		} else if message_value.Action == "STOPPED" {
			log.Printf("Stopping filter with id %d\n", filter.id)

			stop_filter(filter_worker_id, message_value.FilterId)
		} else {
			log.Printf("Invalid action %s\n", message_value.Action)
		}

		// TODO edit filter
	}
}
