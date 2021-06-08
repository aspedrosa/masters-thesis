package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	//_ "github.com/lib/pq"  // final/production
	_ "github.com/mattn/go-sqlite3" // prototype/tests
	"github.com/segmentio/kafka-go"
)

type Subscription struct {
	id int
	selection string
	filter string
}

func main() {
	// database related
	//db_host := os.Getenv("DB_HOST")
	//db_port := os.Getenv("DB_PORT")
	//db_user := os.Getenv("DB_USER")
	//db_password := os.Getenv("DB_PASSWORD")
	//db_name := os.Getenv("DB_NAME")

	// kafka related
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")


	//psqlconn := fmt.Sprintf(
	//	"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
	//	db_host, db_port, db_user, db_password, db_name)

	//db, err := sql.Open("postgres", psqlconn)
	db, err := sql.Open("sqlite3", "database/db.db")
	if err != nil {
		panic("unable to connect to the database")
	}

	/* sqlite3 */
	_, err = db.Exec("DROP TABLE IF EXISTS subscription")
	_, err = db.Exec("CREATE TABLE subscription (id INTEGER PRIMARY KEY AUTOINCREMENT, filter VARCHAR, selection VARCHAR)")
	_, err = db.Exec("INSERT INTO subscription (filter, selection) VALUES ('analysis_id = 0', 'count_value')")

	rows, err := db.Query(`SELECT id, filter, selection from subscription`)

	for rows.Next() {
		var sub Subscription

		err = rows.Scan(&sub.id, &sub.filter, &sub.selection)

		go launch_pipeline(sub)
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(bootstrap_servers, ","),
		GroupID: "subscriptions_manager",
		Topic: "subscriptions_updates",
	})

	for {
		message, _ := consumer.ReadMessage(context.Background())

		fmt.Println(message.Value)
		// parse message

		// 1. create pipeline
		// 2. stop pipeline
		// 3. pause pipeline
	}
}

