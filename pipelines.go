package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
)

var mappings = make(map[int]context.CancelFunc)

var column_types = map[string]string {
	"ANALYSIS_ID": "long",
	"STRATUM_1": "string",
	"STRATUM_2": "string",
	"STRATUM_3": "string",
	"STRATUM_4": "string",
	"STRATUM_5": "string",
	"COUNT_VALUE": "long",
	"MIN_VALUE": "double",
	"AVG_VALUE": "double",
	"MEDIAN_VALUE": "double",
	"P10_VALUE": "double",
	"P25_VALUE": "double",
	"P75_VALUE": "double",
	"P90_VALUE": "double",
}

var all_normal_order = []string {
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

type UploadMessage struct {
	HASH string
	ROWS int64
}

func launch_pipeline(pipeline Pipeline, create_streams bool) {
	// create context to stop pipeline worker
	ctx := context.Background()
	ctx, cancel_pipeline_woker := context.WithCancel(ctx)
	mappings[pipeline.id] = cancel_pipeline_woker

	if create_streams {
		init_streams(pipeline)
	}

	// launch pipeline worker
	go func() {
		bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

		// initiate kafka consumers
		consumer_manager := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d_manager", pipeline.id),
			Topic: "UPLOAD_NOTIFICATIONS",
		})

		consumer := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("pipeline_%d", pipeline.id),
			Topic: fmt.Sprintf("PIPELINE_%d", pipeline.id),
		})

		consumer_not := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("pipeline_%d_not", pipeline.id),
			Topic: fmt.Sprintf("PIPELINE_%d_NOT", pipeline.id),
		})

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

			// read upload notifications messages
			msg, err := consumer_manager.FetchMessage(ctx)
			if err != nil {
				panic("some error happen on manager for subs 1")
			}

			fmt.Println("upload")

			var upload UploadMessage
			err = json.Unmarshal(msg.Value, &upload)
			fmt.Println(err)
			upload.ROWS--

			// launch worker that reads and writes the result of the pipeline to a file
			go func() {
				f, err := os.Create(upload.HASH)
				fmt.Println(err)
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
					if i != len(order) - 1 {
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

						if i != len(order) - 1 {
							f.WriteString(",")
						} else {
							f.WriteString("\n")
						}
					}

					msgs_filtered = append(msgs_filtered, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					counts <- 1
				}

				fmt.Println("worker exiting")

				wg.Done()
			}()

			// launch worker in charge of counting the number of messages that were filtered by the pipeline
			go func() {
				for {
					msg, err := consumer_not.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					msgs_filtered_not = append(msgs_filtered_not, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					counts <- 1
				}

				fmt.Println("worker not exiting")
				wg.Done()
			}()

			processed_messages := int64(0)


			for {
				select {
				case <- ctx.Done():
					cancel_childrens()
					return
				case _ = <- counts:
					processed_messages++
				}

				if processed_messages == upload.ROWS {
					fmt.Println("done")
					cancel_childrens()

					wg.Wait()

					err := consumer_manager.CommitMessages(ctx, msg)
					fmt.Println(err)
					err = consumer.CommitMessages(ctx, msgs_filtered...)
					fmt.Println(err)
					err = consumer_not.CommitMessages(ctx, msgs_filtered_not...)
					fmt.Println(err)

					break
				}
			}

			fmt.Println("out")
		}
	}()
}

func edit_pipeline(new_pipeline Pipeline) {
	stop_pipeline(new_pipeline.id)

	launch_pipeline(new_pipeline, true)
}

func stop_pipeline(pipeline_id int) {
	mappings[pipeline_id]()
	delete(mappings, pipeline_id)

	stop_streams(pipeline_id)
}
