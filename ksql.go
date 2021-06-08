package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func init_stream(sub Subscription, not_stream bool) error {
	//ksql_url := os.Getenv("KSQL_URL")
	ksql_url := "http://localhost:8088"

	var not_suffix string
	if not_stream {
		not_suffix = "_not"
	} else {
		not_suffix = ""
	}

	var selection string
	if not_stream {
		selection = "1"
	} else {
		if sub.selection == "" {
			selection = "*"
		} else {
			selection = sub.selection
		}
	}

	var where string
	if sub.filter == "" {
		where = ""
	} else {
		var begin_not, end_not string
		if not_stream {
			begin_not = "NOT ("
			end_not = ")"
		} else {
			begin_not = ""; end_not = ""
		}
		where = fmt.Sprintf("WHERE %s%s%s", begin_not, sub.filter, end_not)
	}

	post_json_body, _ := json.Marshal(map[string]string{
		"ksql": fmt.Sprintf("CREATE STREAM subscription_%d%s AS SELECT %s FROM DATA %s;", sub.id, not_suffix, selection, where),
	})
	post_body := bytes.NewBuffer(post_json_body)
	response, err := http.Post(fmt.Sprintf("%s/ksql", ksql_url), "application/json", post_body)
	if err != nil {
		return err
	}

	fmt.Println(ioutil.ReadAll(response.Body))

	return nil
}
