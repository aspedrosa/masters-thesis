package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
)

var mappings = make(map[int]context.CancelFunc)

var column_types = map[string]string{
	"ANALYSIS_ID":  "long",
	"STRATUM_1":    "string",
	"STRATUM_2":    "string",
	"STRATUM_3":    "string",
	"STRATUM_4":    "string",
	"STRATUM_5":    "string",
	"COUNT_VALUE":  "long",
	"MIN_VALUE":    "double",
	"AVG_VALUE":    "double",
	"MEDIAN_VALUE": "double",
	"P10_VALUE":    "double",
	"P25_VALUE":    "double",
	"P75_VALUE":    "double",
	"P90_VALUE":    "double",
}

var all_normal_order = []string{
	"ANALYSIS_ID",
	"STRATUM_1",
	"STRATUM_2",
	"STRATUM_3",
	"STRATUM_4",
	"STRATUM_5",
	"COUNT_VALUE",
	"MIN_VALUE",
	"AVG_VALUE",
	"MEDIAN_VALUE",
	"P10_VALUE",
	"P25_VALUE",
	"P75_VALUE",
	"P90_VALUE",
}

var schemaRegistryClient = srclient.CreateSchemaRegistryClient("http://localhost:8081")

func launch_pipeline(pipelines_set int, pipeline Pipeline, create_streams bool) {
	// create context to stop pipeline worker
	ctx := context.Background()
	ctx, cancel_pipeline_woker := context.WithCancel(ctx)
	mappings[pipeline.id] = cancel_pipeline_woker

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
		counts := make(chan byte)
		defer close(counts)
		wg := sync.WaitGroup{}

		// context to stop children workers once all messages were processed
		children_ctx, cancel_childrens := context.WithCancel(ctx)

		for {
			wg.Add(2)

			// read messages that can be committed
			//  these messages objects only hold the necessary values to commit then
			msgs_filtered := make([]kafka.Message, 0, 1500)
			msgs_filtered_not := make([]kafka.Message, 0, 1500)

			var rows uint32

			// read upload notifications messages
			msg, err := consumer_manager.FetchMessage(ctx)
			if err != nil {
				return
			}
			rows = binary.BigEndian.Uint32(msg.Value) - 1

			fmt.Println("upload")

			var filename string
			{
				b := make([]byte, 20)
				rand.Read(b)
				filename = fmt.Sprintf("%x", b)
			}

			// launch worker that reads and writes the result of the pipeline to a file
			go func() {
				defer wg.Done()

				f, _ := os.Create(filename)
				defer f.Close()

				// calculate the file header
				var order []string
				if len(pipeline.selection) == 0 {
					order = all_normal_order
				} else {
					for i, column := range pipeline.selection {
						pipeline.selection[i] = strings.ToUpper(column)
					}
					order = pipeline.selection
				}

				// write the header to the file
				for i, column := range order {
					f.WriteString(column)
					if i != len(order)-1 {
						f.WriteString(",")
					} else {
						f.WriteString("\n")
					}
				}

				for {
					msg, err := consumer.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					fmt.Println("worker read record")

					// parse avro format
					schema_id := binary.BigEndian.Uint32(msg.Value[1:5])
					schema, err := schemaRegistryClient.GetSchema(int(schema_id))
					fmt.Println(err)
					native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
					data := native.(map[string]interface{})

					// write row values to the file
					for i, column := range order {
						column_data := data[column].(map[string]interface{})
						data_type := column_types[column]
						value, exists := column_data[data_type]
						if exists {
							if data_type == "long" {
								f.WriteString(strconv.FormatInt(value.(int64), 10))
							} else if data_type == "double" {
								f.WriteString(strconv.FormatFloat(value.(float64), 'g', -1, 64))
							} else {
								f.WriteString(value.(string))
							}
						}

						if i != len(order)-1 {
							f.WriteString(",")
						} else {
							f.WriteString("\n")
						}
					}

					msgs_filtered = append(msgs_filtered, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					counts <- 1
				}

				fmt.Println("worker exiting")
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
					counts <- 1
				}

				fmt.Println("worker not exiting")
			}()

			processed_messages := uint32(0)

			for {
				select {
				case <-ctx.Done():
					cancel_childrens()
					return
				case _ = <-counts:
					processed_messages++
				}

				if processed_messages == rows {
					fmt.Println("done")
					cancel_childrens()

					wg.Wait()

					err := consumer_manager.CommitMessages(ctx, msg)
					fmt.Println(err)
					err = consumer.CommitMessages(ctx, msgs_filtered...)
					fmt.Println(err)
					err = consumer_not.CommitMessages(ctx, msgs_filtered_not...)
					fmt.Println(err)

					data_ready_notification, _ := json.Marshal(map[string]interface{}{
						"pipeline_id": pipeline.id,
						"filename":    filename,
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

			fmt.Println("out")
		}
	}()
}

func edit_pipeline(pipelines_set int, new_pipeline Pipeline) {
	stop_pipeline(pipelines_set, new_pipeline.id)

	launch_pipeline(pipelines_set, new_pipeline, true)
}

func stop_pipeline(pipelines_set int, pipeline_id int) {
	mappings[pipeline_id]()
	delete(mappings, pipeline_id)

	stop_streams(pipelines_set, pipeline_id)
}
