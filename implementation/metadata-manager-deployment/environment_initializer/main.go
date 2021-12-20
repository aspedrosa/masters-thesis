package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "github.com/segmentio/kafka-go"
    "io/ioutil"
    "log"
    "net"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

var BOOTSTRAP_SERVERS []string
var KSQLDB_URL string

func main() {
    BOOTSTRAP_SERVERS = strings.Split(os.Getenv("BOOTSTRAP_SERVERS"), ",")

    KSQLDB_HOST := os.Getenv("KSQLDB_HOST")
    KSQLDB_PORT := os.Getenv("KSQLDB_PORT")
    KSQLDB_URL = fmt.Sprintf("http://%s:%s", KSQLDB_HOST, KSQLDB_PORT)

    wait_until_active()

    err := create_topics()
    if err != nil {
        log.Fatalln(err)
    }

    err = create_json_streams()
    if err != nil {
        log.Fatalln(err)
    }
    err = create_avro_streams()
    if err != nil {
        log.Fatalln(err)
    }
}

func wait_until_active() {
    duration, _ := time.ParseDuration("5s")

    for {
        response, err := http.Get(KSQLDB_URL)
        if err == nil {
            response, _ := ioutil.ReadAll(response.Body)
            if !strings.Contains(string(response), "KSQL is not yet ready to serve requests") {
                break
            }
        }

        time.Sleep(duration)
    }
}

func create_topics() error {
    conn, err := kafka.Dial("tcp", BOOTSTRAP_SERVERS[0])
    if err != nil {
        return err
    }

    controller, err := conn.Controller()
    if err != nil {
        return err
    }

    var connLeader *kafka.Conn
    connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
    if err != nil {
        return err
    }

    conn.Close()

    controller, err = connLeader.Controller()
    if err != nil {
        return err
    }

    var controllerConn *kafka.Conn
    controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
    if err != nil {
        return err
    }

    controllerConn.CreateTopics(
        []kafka.TopicConfig{
            {
                Topic:             "HEALTH_CHECKS_REQUESTS",
                NumPartitions:     -1,
                ReplicationFactor: -1,
            },
            {
                Topic:             "FILTER_WORKERS_DATA_READY_TO_SEND",
                NumPartitions:     -1,
                ReplicationFactor: -1,
            },
            {
                Topic:             "FILTER_WORKERS_DATA_REQUESTS",
                NumPartitions:     -1,
                ReplicationFactor: -1,
            },
            {
                Topic:             "FILTER_WORKERS_MANAGEMENT",
                NumPartitions:     -1,
                ReplicationFactor: -1,
            },
            {
                Topic:             "SENDERS_MANAGEMENT",
                NumPartitions:     -1,
                ReplicationFactor: -1,
            },
        }...,
    )

    controllerConn.Close()
    connLeader.Close()

    return nil
}

func create_json_streams() error {
    post_json_body, _ := json.Marshal(map[string]interface{}{
        "ksql":
            "CREATE STREAM databases_upload_notifications (\"database_id\" int, \"rows\" int, \"time\" timestamp) WITH (kafka_topic='DATABASES_UPLOAD_NOTIFICATIONS', partitions=1, value_format='json');" +
            "CREATE STREAM sender_statistics (\"application_id\" int, \"time\" timestamp, \"response_code\" int, \"response_data\" string) WITH (kafka_topic='SENDER_STATISTICS', partitions=1, value_format='json');" +
            "CREATE STREAM agents_health_checks (\"database_id\" int, \"time\" timestamp, \"reason\" string) WITH (kafka_topic='AGENTS_HEALTH_CHECKS', partitions=1, value_format='json');",
    })
    post_body := bytes.NewBuffer(post_json_body)
    response, err := http.Post(fmt.Sprintf("%s/ksql", KSQLDB_URL), "application/json", post_body)
    if err != nil {
        return err
    }
    if response.StatusCode != 200 {
        response_bytes, _ := ioutil.ReadAll(response.Body)
        response_str := string(response_bytes)
        return errors.New(response_str)
    }
    return nil
}

func create_avro_streams() error {
    post_json_body, _ := json.Marshal(map[string]interface{}{
        "ksql":
            "CREATE STREAM databases_upload_notifications_avro WITH (value_format='avro') AS SELECT * FROM databases_upload_notifications;" +
            "CREATE STREAM sender_statistics_avro WITH (value_format='avro') AS SELECT * FROM sender_statistics;" +
            "CREATE STREAM agents_health_checks_avro WITH (value_format='avro') AS SELECT * FROM agents_health_checks;",
    })
    post_body := bytes.NewBuffer(post_json_body)
    response, err := http.Post(fmt.Sprintf("%s/ksql", KSQLDB_URL), "application/json", post_body)
    if err != nil {
        return err
    }
    if response.StatusCode != 200 {
        response_bytes, _ := ioutil.ReadAll(response.Body)
        response_str := string(response_bytes)
        return errors.New(response_str)
    }
    return nil
}
