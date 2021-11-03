package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func create_topics(filter_worker_id int, bootstrap_server string) {
	conn, _ := kafka.Dial("tcp", bootstrap_server)

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
				Topic:             fmt.Sprintf("FILTER_WORKER_%d_UPLOAD_NOTIFICATIONS", filter_worker_id),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
			{
				Topic:             fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", filter_worker_id),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
			{
				Topic:             fmt.Sprintf("FILTER_WORKER_%d_DATA_READY_TO_SEND", filter_worker_id),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
		}...,
	)

	controllerConn.Close()
	connLeader.Close()
}
