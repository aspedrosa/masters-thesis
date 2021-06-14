package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"os"
	"strings"
)

type Pipeline struct {
	id int
	selection []string
	filter string
}

func main() {
	// database related
	db_host := os.Getenv("DB_HOST")
	db_port := os.Getenv("DB_PORT")
	db_user := os.Getenv("DB_USER")
	db_password := os.Getenv("DB_PASSWORD")
	db_name := os.Getenv("DB_NAME")

	// kafka related
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

	psqlconn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		db_host, db_port, db_user, db_password, db_name)

	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		panic("unable to connect to the database")
	}

	rows, err := db.Query(`SELECT id, filter FROM pipeline`)

	for rows.Next() {
		var pipeline Pipeline

		err = rows.Scan(&pipeline.id, &pipeline.filter)

		selections, _ := db.Query(`SELECT selection_column FROM pipeline_selection ORDER BY selection_order`)
		var column string
		for selections.Next() {
			selections.Scan(&column)
			pipeline.selection = append(pipeline.selection, column)
		}

		go launch_pipeline(pipeline, false)
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap_servers, ","),
		Topic: "pipelines_updates",
	})

	for {
		message, _ := consumer.ReadMessage(context.Background())

		fmt.Println(message.Value)
		// parse message

		// 1. create pipeline
		// 2. edit pipeline
		// 3. stop pipeline
	}
}

