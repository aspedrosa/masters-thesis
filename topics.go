package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func create_topics() {
	conn, _ := kafka.Dial("tcp", BOOTSTRAP_SERVERS[0])

	controller, _ := conn.Controller()

	var connLeader *kafka.Conn
	connLeader, _ = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

	conn.Close()

	controller, _ = connLeader.Controller()

	var controllerConn *kafka.Conn
	controllerConn, _ = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))

	controllerConn.CreateTopics(
		[]kafka.TopicConfig{
			{
				Topic:             fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", FILTER_WORKER_ID),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
		}...,
	)

	controllerConn.Close()
	connLeader.Close()
}
