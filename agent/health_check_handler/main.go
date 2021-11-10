package main

import (
    "context"
    "encoding/json"
    "fmt"
    "github.com/segmentio/kafka-go"
    "os"
    "strings"
    "sync"
    "time"
)

var BOOTSTRAP_SERVERS []string
var producer *kafka.Writer
var topic string

var requested = false
var requested_time time.Time
var requested_mtx sync.Mutex


func main()  {
    BOOTSTRAP_SERVERS = strings.Split(os.Getenv("CONNECT_BOOTSTRAP_SERVERS"), ",")
    AGENT_DATABASE_IDENTIFIER := os.Getenv("AGENT_DATABASE_IDENTIFIER")

    topic = fmt.Sprintf("db_%s_healthchecks", AGENT_DATABASE_IDENTIFIER)

    producer = &kafka.Writer{
        Addr: kafka.TCP(BOOTSTRAP_SERVERS...),
    }

    requested_mtx = sync.Mutex{}

    go periodic_sender()

    consumer := kafka.NewReader(kafka.ReaderConfig{
        Brokers: BOOTSTRAP_SERVERS,
        Topic: "HEALTH_CHECKS_REQUESTS",
    })
    consumer.SetOffset(kafka.LastOffset)

    for {
        msg, _ := consumer.FetchMessage(context.Background())

        if string(msg.Value) == AGENT_DATABASE_IDENTIFIER {
            send_health_check("Requested")

            requested_mtx.Lock()
            requested = true
            requested_time = time.Now()
            requested_mtx.Unlock()
        }
    }
}

func periodic_sender()  {
    sleep_duration, _ := time.ParseDuration("336h")  // TODO maybe receive this as an argument

    for {
        requested_mtx.Lock()
        if requested {
            requested = false
            sleep_duration = time.Now().Sub(requested_time.Add(sleep_duration))
        } else {
            send_health_check("Period")
        }
        requested_mtx.Unlock()

        time.Sleep(sleep_duration)
    }
}

func send_health_check(reason string) {
    health_check, _ := json.Marshal(map[string]interface{}{
        "time": time.Now().UnixMilli(),
        "reason": reason,
    })

    producer.WriteMessages(
        context.Background(),
        kafka.Message{
            Topic: topic,
            Value: health_check,
        },
    )
}