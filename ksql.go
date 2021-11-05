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

var schemaRegistryClient *srclient.SchemaRegistryClient

func Init_schema_registry_client() {
	schemaRegistryClient = srclient.CreateSchemaRegistryClient(SCHEMA_REGISTRY_URL)
}

func Init_data_stream() error {
	data_topic := fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", FILTER_WORKER_ID)

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DESCRIBE %s;", data_topic),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", KSQLDB_URL), "application/json", post_body)
	if err != nil {
		return err
	} else if response.StatusCode == 400 {
		// assume that if fails the data stream doesn't exist

		var schema *srclient.Schema
		var err error

		schema_name := fmt.Sprintf("%s-value", data_topic)
		schema, err = schemaRegistryClient.GetLatestSchema(schema_name)
		if err != nil {
			f, _ := os.Open("filter_worker_data_topics_schema.json")
			schema_json, _ := ioutil.ReadAll(f)
			f.Close()
			schema, err = schemaRegistryClient.CreateSchema(schema_name, string(schema_json), srclient.Avro)
			if err != nil {
				return nil
			}
		}

		post_json_body, _ = json.Marshal(map[string]interface{}{
			"ksql": fmt.Sprintf(
				"CREATE STREAM %s WITH (kafka_topic='%s', value_format='avro', value_schema_id=%d);",
				data_topic, data_topic, schema.ID(),
			),
		})
		post_body = bytes.NewBuffer(post_json_body)
		response, err = http.Post(fmt.Sprintf("%s/ksql", KSQLDB_URL), "application/json", post_body)
		if err != nil {
			return err
		} else if response.StatusCode != 200 {
			response_bytes, _ := ioutil.ReadAll(response.Body)
			response_str := string(response_bytes)
			return errors.New(response_str)
			//return errors.New("creation of data stream failed")
		}
	}

	return nil
}

func Init_streams(filter Filter) error {
	data_topic := fmt.Sprintf("FILTER_WORKER_%d_DATA_TO_PARSE", FILTER_WORKER_ID)

	var selection string
	if len(filter.Selections) == 0 {
		selection = "*"
	} else {
		selection = strings.Join(filter.Selections, ", ")
	}

	var where string
	var where_not string
	if filter.Filter == "" {
		where = ""
		where_not = ""
	} else {
		where = fmt.Sprintf("WHERE %s", filter.Filter)
		where_not = fmt.Sprintf("WHERE NOT(%s)", filter.Filter)
	}

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"CREATE STREAM FILTER_WORKER_%d_FILTER_%d AS SELECT %s FROM %s %s;"+
				"CREATE STREAM FILTER_WORKER_%d_FILTER_%d_NOT AS SELECT 1 FROM %s %s;",
			FILTER_WORKER_ID, filter.Id, selection, data_topic, where,
			FILTER_WORKER_ID, filter.Id, data_topic, where_not,
		),
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

func Stop_streams(filter_id int) error {
	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf(
			"DROP STREAM FILTER_WORKER_%d_FILTER_%d;"+
				"DROP STREAM FILTER_WORKER_%d_FILTER_%d_NOT;",
			FILTER_WORKER_ID, filter_id, FILTER_WORKER_ID, filter_id,
		),
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
