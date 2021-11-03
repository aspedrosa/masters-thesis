package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/riferrei/srclient"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

var schemaRegistryClient = srclient.CreateSchemaRegistryClient("http://localhost:8081") // don't hardcode

func init_data_stream(filter_worker_id int) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088" // TODO don't hardcode

	data_topic := fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", filter_worker_id)

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DESCRIBE %s;", data_topic),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	} else if response.StatusCode == 400 {
		// assume that if fails the data stream doesn't exist

		_, err := schemaRegistryClient.GetLatestSchema(data_topic)
		if err != nil {
			f, _ := os.Open("filter_worker_data_topics_schema.json")
			schema_json, _ := ioutil.ReadAll(f)
			f.Close()
			_, err := schemaRegistryClient.CreateSchema(data_topic, string(schema_json), srclient.Avro)
			if err != nil {
				return nil
			}
		}

		post_json_body, _ = json.Marshal(map[string]interface{}{
			"ksql": fmt.Sprintf("CREATE STREAM %s WITH (kafka_topic='%s', value_format='avro');", data_topic, data_topic),
		})
		post_body = bytes.NewBuffer(post_json_body)
		response, err = http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
		if err != nil {
			return err
		} else if response.StatusCode != 200 {
			return errors.New("creation of data stream failed")
		}
	}

	return nil
}

func init_streams(filter_worker_id int, filter Filter) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088" // TODO dont' hardcode

	data_topic := fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", filter_worker_id)

	var selection string
	if len(filter.selection) == 0 {
		selection = "*"
	} else {
		selection = strings.Join(filter.selection, ", ")
	}

	var where string
	var where_not string
	if filter.filter == "" {
		where = ""
		where_not = ""
	} else {
		where = fmt.Sprintf("WHERE %s", filter.filter)
		where_not = fmt.Sprintf("WHERE NOT(%s)", filter.filter)
	}

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"CREATE STREAM FILTER_WORKER_%d_FILTER_%d AS SELECT %s FROM %s %s;"+
				"CREATE STREAM FILTER_WORKER_%d_FILTER_%d_NOT AS SELECT 1 FROM %s %s;",
			filter_worker_id, filter.id, selection, data_topic, where,
			filter_worker_id, filter.id, data_topic, where_not,
		),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
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

func stop_streams(filter_worker_id int, filter_id int) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"DROP STREAM FILTER_WORKER_%d_FILTER_%d;"+
				"DROP STREAM FILTER_WORKER_%d_FILTER_%d_NOT;",
			filter_worker_id, filter_id, filter_worker_id, filter_id,
		),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
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
