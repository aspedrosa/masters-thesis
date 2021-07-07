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

func launch_pipeline(pipelines_set int, pipeline Pipeline, create_streams bool) {
	// create context to stop pipeline worker
	ctx := context.Background()
	ctx, cancel_pipeline_woker := context.WithCancel(ctx)

	mappings_mtx.Lock()
	mappings[pipeline.id] = cancel_pipeline_woker
	mappings_mtx.Unlock()

	if create_streams {
		init_streams(pipelines_set, pipeline)
	}

	// launch pipeline worker
	go func() {
		bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

		// initiate kafka consumers
		consumer_manager := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d_manager", pipeline.id),
			Topic:   fmt.Sprintf("PIPELINES_SET_%d_UPLOAD_NOTIFICATIONS", pipelines_set),
		})

		consumer := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("pipeline_%d", pipeline.id),
			Topic:   fmt.Sprintf("PIPELINES_SET_%d_PIPELINE_%d", pipelines_set, pipeline.id),
		})

		consumer_not := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("pipeline_%d_not", pipeline.id),
			Topic:   fmt.Sprintf("PIPELINES_SET_%d_PIPELINE_%d_NOT", pipelines_set, pipeline.id),
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
			sent_rows = binary.BigEndian.Uint32(msg.Value) - 1

			log.Printf("Pipeline %d received an upload\n", pipeline.id)

			// launch worker that reads and writes the result of the pipeline to a file
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

				log.Printf("Worker for pipeline %d exiting\n", pipeline.id)
			}()

			// launch worker in charge of counting the number of messages that were filtered by the pipeline
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

				log.Printf("WorkerNot for pipeline %d exiting\n", pipeline.id)
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
					log.Printf("All records processed on pipeline %d\n", pipeline.id)
					cancel_childrens()

					wg.Wait()

					consumer_manager.CommitMessages(ctx, msg)
					consumer.CommitMessages(ctx, msgs_filtered...)
					consumer_not.CommitMessages(ctx, msgs_filtered_not...)

					log.Printf("Sending DATA_READY_TO_SEND message from pipeline %d\n", pipeline.id)

					data_ready_notification, _ := json.Marshal(map[string]interface{}{
						"pipeline_id":   pipeline.id,
						"pipelines_set": pipelines_set,
						"last_offset":   last_offset,
						"count":         filtered_count,
					})
					data_ready_to_send_producer.WriteMessages(
						ctx,
						kafka.Message{
							Topic: fmt.Sprintf("PIPELINES_SET_%d_DATA_READY_TO_SEND", pipelines_set),
							Value: data_ready_notification,
						},
					)

					break
				}
			}

			log.Printf("All done on pipeline %d for this upload\n", pipeline.id)
		}
	}()
}

func edit_pipeline(pipelines_set int, new_pipeline Pipeline) {
	stop_pipeline(pipelines_set, new_pipeline.id)

	launch_pipeline(pipelines_set, new_pipeline, true)
}

func stop_pipeline(pipelines_set int, pipeline_id int) {
	mappings[pipeline_id]()
	mappings_mtx.Lock()
	delete(mappings, pipeline_id)
	mappings_mtx.Unlock()

	stop_streams(pipelines_set, pipeline_id)
}
