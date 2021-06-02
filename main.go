package main

import (
	"os"

	"github.com/segmentio/kafka-go"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	bootstrap_servers := os.Getenv("BOOTSTRAP_SERVERS")

}
