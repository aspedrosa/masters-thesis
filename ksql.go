package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

func create_database_status_streams(hash string) error {
	KSQL_URL := os.Getenv("KSQL_URL")

	db_topic_name := fmt.Sprintf("db_%s_status", hash)

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"CREATE STREAM %s (status VARCHAR, offset STRUCT<rows BIGINT>) "+
				"WITH (kafka_topic='%s', value_format='json');\n"+
				db_topic_name, db_topic_name,
		),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", KSQL_URL), "application/json", post_body)
	if err != nil {
		return err
	} else if response.StatusCode == 400 {
		// db_topic does not exist. Ok, then lets create the stream and the underlying topic
		post_json_body, _ := json.Marshal(map[string]interface{}{
			"ksql": fmt.Sprintf(
				"CREATE STREAM %s (status VARCHAR, offset STRUCT<rows BIGINT>) "+
					"WITH (kafka_topic='%s', value_format='json', partitions=1);\n",
				db_topic_name, db_topic_name,
			),
			"streamsProperties": map[string]interface{}{
				"ksql.streams.replication.factor": "1",
			},
		})
		post_body := bytes.NewBuffer(post_json_body)
		response, err = http.Post(fmt.Sprintf("%s/ksql", KSQL_URL), "application/json", post_body)
		if err != nil {
			return err
		} else if response.StatusCode != 200 {
			response_bytes, _ := ioutil.ReadAll(response.Body)
			response_str := string(response_bytes)
			return errors.New(response_str)
		}
	} else if response.StatusCode != 200 {
		response_bytes, _ := ioutil.ReadAll(response.Body)
		response_str := string(response_bytes)
		return errors.New(response_str)
	}

	post_json_body, _ = json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"CREATE STREAM upload_notifications_%s "+
				"WITH (kafka_topic='DATABASES_UPLOAD_NOTIFICATIONS') "+
				"AS SELECT '%s' as hash, offset->rows "+
				"FROM %s "+
				"WHERE status = 'COMPLETED';",
			hash, hash, db_topic_name,
		),
	})
	post_body = bytes.NewBuffer(post_json_body)
	response, err = http.Post(fmt.Sprintf("%s/ksql", KSQL_URL), "application/json", post_body)
	if err != nil {
		return err
	} else if response.StatusCode != 200 {
		response_bytes, _ := ioutil.ReadAll(response.Body)
		response_str := string(response_bytes)
		return errors.New(response_str)
	}

	return nil
}
