package main

import (
    "bytes"
    "encoding/json"
    "errors"
    "fmt"
    "io/ioutil"
    "log"
    "net/http"
    "os"
    "time"
)

var KSQLDB_URL string

func main() {
    KSQLDB_HOST := os.Getenv("KSQLDB_HOST")
    KSQLDB_PORT := os.Getenv("KSQLDB_PORT")
    KSQLDB_URL = fmt.Sprintf("http://%s:%s", KSQLDB_HOST, KSQLDB_PORT)

    wait_until_active()

    err := create_json_streams()
    if err != nil {
        log.Fatalln(err)
    }
    err = create_avro_streams()
    if err != nil {
        log.Fatalln(err)
    }
}

func wait_until_active() {
    for {
        _, err := http.Get(KSQLDB_URL)
        if err != nil {
            duration, _ := time.ParseDuration("5s")
            time.Sleep(duration)
        } else {
            break
        }
    }
}

func create_json_streams() error {
    post_json_body, _ := json.Marshal(map[string]interface{}{
        "ksql":
            "CREATE STREAM databases_upload_notifications (database_id int, rows int, time timestamp) WITH (kafka_topic='DATABASES_UPLOAD_NOTIFICATIONS', partitions=1, value_format='json');" +
            "CREATE STREAM sender_statistics (application_id int, time timestamp, response_code int, response_data string) WITH (kafka_topic='SENDER_STATISTICS', partitions=1, value_format='json');" +
            "CREATE STREAM agents_health_checks (database_id int, time timestamp, reason string) WITH (kafka_topic='AGENTS_HEALTH_CHECKS', partitions=1, value_format='json');",
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
