package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
)

var mappings = make(map[int]context.CancelFunc)

var column_types = map[string]string {
	"ANALYSIS_ID": "Long",
	"STRATUM_1": "string",
	"STRATUM_2": "string",
	"STRATUM_3": "string",
	"STRATUM_4": "string",
	"STRATUM_5": "string",
	"COUNT_VALUE": "long",
	"MIN_VALUE": "long",
	"AVG_VALUE": "string",
	"MEDIAN_VALUE": "string",
	"P10_VALUE": "long",
	"P25_VALUE": "long",
	"P75_VALUE": "long",
	"P90_VALUE": "long",
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

func launch_pipeline(sub Subscription) {
	//init_stream(sub, false)
	//init_stream(sub, true)

	ctx := context.Background()

	ctx, cancel_woker := context.WithCancel(ctx)
	mappings[sub.id] = cancel_woker

	go func() {
		bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

		consumer_manager := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d_manager", sub.id),
			Topic: "UPLOAD_NOTIFICATIONS",
		})

		consumer := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d", sub.id),
			Topic: fmt.Sprintf("SUBSCRIPTION_%d", sub.id),
		})

		consumer_not := kafka.NewReader(kafka.ReaderConfig{
			Brokers: strings.Split(bootstrap_servers, ","),
			GroupID: fmt.Sprintf("subscription_%d_not", sub.id),
			Topic: fmt.Sprintf("SUBSCRIPTION_%d_NOT", sub.id),
		})

		counts := make(chan byte)
		defer close(counts)

		children_ctx, cancel_childrens := context.WithCancel(ctx)

		for {
			commit := false
			msg, err := consumer_manager.FetchMessage(ctx)
			if err != nil {
				panic("some error happen on manager for subs 1")
			}

			// todo parse message value
			sent := 1398
			db_hash := "aslkdfhalksdfha"

			go func() {
				f, _ := os.Create(db_hash)
				defer f.Close()

				var order []string
				if sub.selection == "" {
					order = all_normal_order
				} else {
					order = strings.Split(sub.selection, ",")
					for i, column := range order {
						column = strings.Trim(column, " ")
						column = strings.ToUpper(column)
						order[i] = column
					}
				}

				for i, column := range order {
					f.WriteString(column)
					if i != len(order) - 1 {
						f.WriteString(",")
					} else {
						f.WriteString("\n")
					}
				}

				var msgs []kafka.Message

				for {
					msg, err := consumer.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					schema_id := binary.BigEndian.Uint32(msg.Value[1:5])
					schema, err := schemaRegistryClient.GetSchema(int(schema_id))
					native, _, _ := schema.Codec().NativeFromBinary(msg.Value[5:])
					data := native.(map[string]interface{})

					for i, column := range order {
						column_data := data[column].(map[string]interface{})
						data_type := column_types[column]
						value, exists := column_data[data_type]
						if exists {
							if data_type == "long" {
								f.WriteString(strconv.FormatInt(value.(int64), 10))
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

					msgs = append(msgs, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					counts <- 1
				}

				if commit {
					consumer.CommitMessages(children_ctx, msgs...)
				}
			}()

			go func() {  // not
				var msgs []kafka.Message

				for {
					msg, err := consumer_not.FetchMessage(children_ctx)
					if err != nil {
						break
					}

					msgs = append(msgs, kafka.Message{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset})
					counts <- 1
				}

				if commit {
					consumer.CommitMessages(children_ctx, msgs...)
				}
			}()

			processed_messages := 0

			for {
				select {
				case <- ctx.Done():
					cancel_childrens()
					return
				case _ = <- counts:
					processed_messages++
					if processed_messages == sent {
						commit = true
						consumer_manager.CommitMessages(ctx, msg)
						cancel_childrens()
					}
				}
			}
		}
	}()
}

func edit_pipeline(sub Subscription) {

}

func pause_pipeline(sub Subscription) {

}

func drop_pipeline(sub Subscription) {

}
