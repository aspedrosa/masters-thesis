package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/segmentio/kafka-go"
	"os"
	"strings"
)

type Pipeline struct {
	id        int
	selection []string
	filter    string
}

func main() {
	pipelines_set := 0

	// kafka related
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

	create_topics(pipelines_set, bootstrap_servers)
	init_data_stream(pipelines_set)

	// database related
	//db_host := os.Getenv("DB_HOST")
	//db_port := os.Getenv("DB_PORT")
	//db_user := os.Getenv("DB_USER")
	//db_password := os.Getenv("DB_PASSWORD")
	//db_name := os.Getenv("DB_NAME")

	//psqlconn := fmt.Sprintf(
	//	"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
	//	db_host, db_port, db_user, db_password, db_name)

	//db, err := sql.Open("postgres", psqlconn)
	db, err := sql.Open("sqlite3", "demo.db")
	if err != nil {
		panic("unable to connect to the database")
	}

	rows, _ := db.Query(`SELECT id, filter FROM pipelines`)
	for rows.Next() {
		var pipeline Pipeline

		rows.Scan(&pipeline.id, &pipeline.filter)

		selections, _ := db.Query(
			`SELECT column FROM pipeline_selections WHERE pipeline_id = ? ORDER BY "order"`,
			pipeline.id,
		)

		var column string
		for selections.Next() {
			selections.Scan(&column)
			pipeline.selection = append(pipeline.selection, column)
		}

		go launch_pipeline(pipelines_set, pipeline, false)
	}

	db.Close()

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap_servers, ","),
		Topic:   "PIPELINES_MANAGEMENT",
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
