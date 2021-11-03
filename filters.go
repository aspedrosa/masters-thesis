package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

var mappings_mtx = sync.Mutex{}
var mappings = make(map[int]context.CancelFunc)

func launch_filter(filter_worker_id int, filter Filter, create_streams bool) {
	// create context to stop filter worker
	ctx := context.Background()
	ctx, cancel_filter_main := context.WithCancel(ctx)

	mappings_mtx.Lock()
	mappings[filter.id] = cancel_filter_main
	mappings_mtx.Unlock()

	if create_streams {
		init_streams(filter_worker_id, filter)
	}

	// launch filter worker
	go func() {
		bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

		// initiate kafka consumers
		consumer_manager := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d_manager", filter.id),
			Topic:   fmt.Sprintf("FILTER_WORKER_%d_UPLOAD_NOTIFICATIONS", filter_worker_id),
		})

		consumer := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("filter_%d", filter.id),
			Topic:   fmt.Sprintf("FILTER_WORKER_%d_FILTER_%d", filter_worker_id, filter.id),
		})

		consumer_not := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("filter_%d_not", filter.id),
			Topic:   fmt.Sprintf("FILTER_WORKER_%d_FILTER_%d_NOT", filter_worker_id, filter.id),
		})

		data_ready_to_send_producer := &kafka.Writer{
			Addr: kafka.TCP(strings.Split(bootstrap_servers, ",")...),
		}

		// channels so children workers communicate received messages
		filtered := make(chan byte)
		filtered_not := make(chan byte)
		defer close(filtered)
		defer close(filtered_not)

		// wait group for main worker wait for both children to exit
		wg := sync.WaitGroup{}

		for {
			// context to stop children workers once all messages were processed
			children_ctx, cancel_childrens := context.WithCancel(ctx)

			wg.Add(2)

			// read messages that can be committed
			//  these messages objects only hold the necessary values to commit then
			msgs_filtered := make([]kafka.Message, 0, 1500)
			msgs_filtered_not := make([]kafka.Message, 0, 1500)

			var sent_rows uint32
			var last_offset int64

			// read upload notifications messages
			msg, err := consumer_manager.FetchMessage(ctx)
			if err != nil {
				return
			}

			// TODO check if this filter is associated with the community
			community_checks_out := true

			sent_rows = binary.BigEndian.Uint32(msg.Value) - 1

			log.Printf("Filter %d received an upload\n", filter.id)

			// launch worker that reads and writes the result of the filter to a file
			go func() {
				defer wg.Done()

				for {
					msg, err := consumer.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					msgs_filtered = append(msgs_filtered, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					last_offset = msg.Offset
					filtered <- 1
				}

				log.Printf("Worker for filter %d exiting\n", filter.id)
			}()

			// launch worker in charge of counting the number of messages that were filtered by the filter
			go func() {
				defer wg.Done()

				for {
					msg, err := consumer_not.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					msgs_filtered_not = append(msgs_filtered_not, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					filtered_not <- 1
				}

				log.Printf("WorkerNot for filter %d exiting\n", filter.id)
			}()

			filtered_count := uint32(0)
			filtered_not_count := uint32(0)

			for {
				select {
				case <-ctx.Done():
					cancel_childrens()
					return
				case _ = <-filtered:
					filtered_count++
				case _ = <-filtered_not:
					filtered_not_count++
				}

				if filtered_count+filtered_not_count == sent_rows {
					log.Printf("All records processed on filter %d\n", filter.id)
					cancel_childrens()

					wg.Wait()

					consumer_manager.CommitMessages(ctx, msg)
					consumer.CommitMessages(ctx, msgs_filtered...)
					consumer_not.CommitMessages(ctx, msgs_filtered_not...)

					if community_checks_out {
						log.Printf("Sending DATA_READY_TO_SEND message from filter %d\n", filter.id)

						data_ready_notification, _ := json.Marshal(map[string]interface{}{
							"filter_id":        filter.id,
							"filter_worker_id": filter_worker_id,
							"last_offset":      last_offset,
							"count":            filtered_count,
						})
						data_ready_to_send_producer.WriteMessages(
							ctx,
							kafka.Message{
								Topic: fmt.Sprintf("FILTER_WORKER_%d_DATA_READY_TO_SEND", filter_worker_id),
								Value: data_ready_notification,
							},
						)
					}

					break
				}
			}

			log.Printf("All done on filter %d for this upload\n", filter.id)
		}
	}()
}

func edit_filter(filter_worker_id int, new_filter_id Filter) {
	stop_filter(filter_worker_id, new_filter_id.id)

	launch_filter(filter_worker_id, new_filter_id, true)
}

func stop_filter(filter_worker_id int, filter_id int) {
	mappings[filter_id]()
	mappings_mtx.Lock()
	delete(mappings, filter_id)
	mappings_mtx.Unlock()

	stop_streams(filter_worker_id, filter_id)
}
