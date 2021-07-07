package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"log"

	//_ "github.com/mattn/go-sqlite3"
	"github.com/segmentio/kafka-go"
	"math/rand"
	"os"
	"strings"
	"time"
)

type Pipeline struct {
	id        int
	selection []string
	filter    string
}

type ManagementMessage struct {
	PipelineId int    `json:"pipeline_id"`
	Action     string `json:"action"`
}

func main() {
	pipelines_set := 0 // TODO get this as a program argument

	// kafka related
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

	create_topics(pipelines_set, bootstrap_servers)
	init_data_stream(pipelines_set)

	// database related
	db_host := os.Getenv("DB_HOST")
	db_port := os.Getenv("DB_PORT")
	db_user := os.Getenv("DB_USER")
	db_password := os.Getenv("DB_PASSWORD")
	db_name := os.Getenv("DB_NAME")

	psqlconn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		db_host, db_port, db_user, db_password, db_name)

	db, err := sql.Open("postgres", psqlconn)
	//db, err := sql.Open("sqlite3", "demo.db")
	if err != nil {
		panic("unable to connect to the database")
	}

	rand.Seed(time.Now().UTC().UnixNano())

	rows, _ := db.Query(`SELECT id, filter FROM pipelines WHERE status = 'ACTIVE'`)
	for rows != nil && rows.Next() {
		var pipeline Pipeline

		rows.Scan(&pipeline.id, &pipeline.filter)

		selections, err := db.Query(
			fmt.Sprintf(
				`SELECT selection_column FROM pipeline_selections WHERE pipeline_id = %d ORDER BY selection_order`,
				pipeline.id,
			),
		)
		if err != nil {
			log.Fatal(err)
		}

		var column string
		for selections.Next() {
			selections.Scan(&column)
			pipeline.selection = append(pipeline.selection, column)
		}

		go launch_pipeline(pipelines_set, pipeline, false)
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap_servers, ","),
		Topic:   "PIPELINES_MANAGEMENT",
	})

	for {
		message, _ := consumer.ReadMessage(context.Background())

		var message_value ManagementMessage
		err := json.Unmarshal(message.Value, &message_value)
		fmt.Println(err)

		var pipeline Pipeline
		pipeline.id = message_value.PipelineId

		if message_value.Action == "ACTIVE" {
			row, err := db.Query(fmt.Sprintf("SELECT filter FROM pipeline WHERE id = %d", message_value.PipelineId))
			row.Next()
			row.Scan(&pipeline.filter)

			selections, err := db.Query(
				fmt.Sprintf(
					`SELECT selection_column FROM pipeline_selections WHERE pipeline_id = %d ORDER BY selection_order`,
					pipeline.id,
				),
			)
			if err != nil {
				log.Fatal(err)
			}

			var column string
			for selections.Next() {
				selections.Scan(&column)
				pipeline.selection = append(pipeline.selection, column)
			}

			launch_pipeline(pipelines_set, pipeline, true)

		} else if message_value.Action == "STOPPED" {
			stop_pipeline(pipelines_set, message_value.PipelineId)
		} else {
			fmt.Printf("Invalid action %s\n", message_value.Action)
		}

		// TODO edit pipeline
	}
}
