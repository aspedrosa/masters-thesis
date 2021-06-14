package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

func init_streams(pipeline Pipeline) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

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
		"ksql": fmt.Sprintf("CREATE STREAM subscription_%d AS SELECT %s FROM DATA %s;", pipeline.id, selection, where),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	post_json_body, _ = json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("CREATE STREAM subscription_%d_not AS SELECT 1 FROM DATA %s;", pipeline.id, where_not),
	})
	post_body = bytes.NewBuffer(post_json_body)
	response, err = http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	return nil
}

func stop_streams(pipeline_id int) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	// drop main stream
	post_json_body, _ := json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DROP STREAM subscription_%d;", pipeline_id),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	// drop not stream
	post_json_body, _ = json.Marshal(map[string]interface{}{
		"ksql": fmt.Sprintf("DROP STREAM subscription_%d_not;", pipeline_id),
	})
	post_body = bytes.NewBuffer(post_json_body)
	response, err = http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}
	fmt.Println(ioutil.ReadAll(response.Body))

	return nil
}
