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

func init_data_stream(pipelines_set int) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	data_topic := fmt.Sprintf("PIPELINES_SET_%d_DATA_TO_PARSE", pipelines_set)

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DESCRIBE %s;", data_topic),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	} else if response.StatusCode == 400 {
		// assume that if fails the data stream doesn't exist

		_, err := schemaRegistryClient.GetLatestSchema(data_topic, false)
		if err != nil {
			f, _ := os.Open("pipelines_sets_data_topics_schema.json")
			schema_json, _ := ioutil.ReadAll(f)
			f.Close()
			_, err := schemaRegistryClient.CreateSchema(data_topic, string(schema_json), srclient.Avro, false)
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

func init_streams(pipelines_set int, pipeline Pipeline) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	data_topic := fmt.Sprintf("PIPELINES_SET_%d_DATA_TO_PARSE", pipelines_set)

	var selection string
	if len(pipeline.selection) == 0 {
		selection = "*"
	} else {
		selection = strings.Join(pipeline.selection, ", ")
	}

	var where string
	var where_not string
	if pipeline.filter == "" {
		where = ""
		where_not = ""
	} else {
		where = fmt.Sprintf("WHERE %s", pipeline.filter)
		where_not = fmt.Sprintf("WHERE NOT(%s)", pipeline.filter)
	}

	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("CREATE STREAM PIPELINES_SET_%d_PIPELINE_%d AS SELECT %s FROM %s %s;",
			pipelines_set, pipeline.id, selection, data_topic, where),
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

	post_json_body, _ = json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("CREATE STREAM PIPELINES_SET_%d_PIPELINE_%d_NOT AS SELECT 1 FROM %s %s;",
			pipelines_set, pipeline.id, data_topic, where_not),
	})
	post_body = bytes.NewBuffer(post_json_body)
	response, err = http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
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

func stop_streams(pipelines_set int, pipeline_id int) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	// drop main stream
	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DROP STREAM PIPELINES_SET_%d_PIPELINE_%d;", pipelines_set, pipeline_id),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	// drop not stream
	post_json_body, _ = json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DROP STREAM PIPELINES_SET_%d_PIPELINE_%d_NOT;", pipelines_set, pipeline_id),
	})
	post_body = bytes.NewBuffer(post_json_body)
	response, err = http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	return nil
}
