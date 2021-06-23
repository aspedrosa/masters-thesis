package main

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

func create_topics(pipeline_set int, bootstrap_server string) {
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
				Topic:             fmt.Sprintf("PIPELINES_SET_%d_UPLOAD_NOTIFICATIONS", pipeline_set),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
			{
				Topic:             fmt.Sprintf("PIPELINES_SET_%d_DATA_TO_PARSE", pipeline_set),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
			{
				Topic:             fmt.Sprintf("PIPELINES_SET_%d_DATA_READY_TO_SEND", pipeline_set),
				NumPartitions:     -1,
				ReplicationFactor: -1,
			},
		}...,
	)

	controllerConn.Close()
	connLeader.Close()
}
