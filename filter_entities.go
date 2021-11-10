package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
)

func filter_main(
	filter_id int,
	empty_filter bool,
	upload_notification_chan chan UploadToFilter,
	ctx context.Context,
) {
	// initiate kafka consumers
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: BOOTSTRAP_SERVERS,
		GroupID: fmt.Sprintf("filter_%d", filter_id),
		Topic:   fmt.Sprintf("FILTER_WORKER_%d_FILTER_%d", FILTER_WORKER_ID, filter_id),
	})

	var consumer_not *kafka.Reader
	if !empty_filter {
		consumer_not = kafka.NewReader(kafka.ReaderConfig{
			Brokers: BOOTSTRAP_SERVERS,
			GroupID: fmt.Sprintf("filter_%d_not", filter_id),
			Topic:   fmt.Sprintf("FILTER_WORKER_%d_FILTER_%d_NOT", FILTER_WORKER_ID, filter_id),
		})
	}

	data_ready_to_send_producer := &kafka.Writer{
		Addr: kafka.TCP(BOOTSTRAP_SERVERS...),
	}

	// channels so children workers communicate received messages
	filtered := make(chan struct{})
	var filtered_not chan struct{}
	if !empty_filter {
		filtered_not = make(chan struct{})
	}

	// wait group for main worker wait for both children to exit
	wg := sync.WaitGroup{}

	for {
		if !empty_filter {
			wg.Add(2)
		} else {
			wg.Add(1)
		}

		// read messages that can be committed
		//  these messages objects only hold the necessary values to commit then
		msgs_filtered := make([]kafka.Message, 0, 1500)
		var msgs_filtered_not []kafka.Message
		if !empty_filter {
			msgs_filtered_not = make([]kafka.Message, 0, 1500)
		}

		var upload UploadToFilter
		var last_offset int64

		// read upload notifications messages
		select {
		case upload = <-upload_notification_chan:
		case <-ctx.Done():
			return
		}

		// context to stop children workers once all messages were processed
		children_ctx, cancel_childrens := context.WithCancel(ctx)

		log.Printf("Filter %d received an upload\n", filter_id)

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
				filtered <- struct{}{}
			}

			log.Printf("Worker for filter %d exiting\n", filter_id)
		}()

		if !empty_filter {
			// launch worker in charge of counting the number of messages that were filtered by the filter
			go func() {
				defer wg.Done()

				for {
					msg, err := consumer_not.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					msgs_filtered_not = append(msgs_filtered_not, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					filtered_not <- struct{}{}
				}

				log.Printf("WorkerNot for filter %d exiting\n", filter_id)
			}()
		}

		filtered_count := uint32(0)
		filtered_not_count := uint32(0)

		for {
			select {
			case <-ctx.Done():
				cancel_childrens()
				if Waiting_for_filters {
					Filters_wait_group.Done()
				}
				return
			case <-filtered:
				filtered_count++
			case <-filtered_not:
				filtered_not_count++
			}

			if filtered_count+filtered_not_count == upload.Rows {
				log.Printf("All records processed on filter %d\n", filter_id)

				break
			}
		}

		cancel_childrens()

		wg.Wait()

		consumer.CommitMessages(ctx, msgs_filtered...)
		if !empty_filter {
			consumer_not.CommitMessages(ctx, msgs_filtered_not...)
		}

		if upload.Belongs_to_communities {
			log.Printf("Sending DATA_READY_TO_SEND message from filter %d\n", filter_id)

			data_ready_notification, _ := json.Marshal(map[string]interface{}{
				"filter_worker_id":    FILTER_WORKER_ID,
				"filter_id":           filter_id,
				"database_identifier": upload.Database_identifier,
				"last_offset":         last_offset,
				"count":               filtered_count,
			})
			data_ready_to_send_producer.WriteMessages(
				ctx,
				kafka.Message{
					Topic: fmt.Sprintf("FILTER_WORKERS_DATA_READY_TO_SEND"),
					Value: data_ready_notification,
				},
			)
		}

		Filters_wait_group.Done()

		log.Printf("All done on filter %d for this upload\n", filter_id)
	}
}
