package main

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"os"
)

func get_env_var(name string) string {
	value, exists := os.LookupEnv(name)
	if !exists {
		panic(fmt.Sprintf("Mandatory environment variable %s was not provided", name))
	}
	return value
}

func main() {
	db_host := get_env_var("DB_HOST")
	db_port := get_env_var("DB_PORT")
	db_user := get_env_var("DB_USER")
	db_password := get_env_var("DB_PASSWORD")
	db_name := get_env_var("DB_NAME")
	get_env_var("BOOTSTRAP_SERVERS")
	get_env_var("KSQL_URL")

	DB, err := gorm.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			db_host, db_port, db_user, db_password, db_name,
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	Admin := configure_admin_models(DB)

	// initialize an HTTP request multiplexer
	mux := http.NewServeMux()

	// Mount admin interface to mux
	Admin.MountTo("/admin", mux)

	fmt.Println("Listening on: 9000")
	http.ListenAndServe(":9000", mux)
}
