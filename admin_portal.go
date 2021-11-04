package main

import (
	"./globals"
	"./shared_structs"

	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

var login_token string
var client = http.Client{}

func get_new_login_token() {
	resp, _ := http.Post(
		fmt.Sprintf("%s/token/", globals.ADMIN_PORTAL_URL),
		"application/json",
		bytes.NewBuffer([]byte(fmt.Sprintf(
			"{\"username\":\"%s\", \"password\":\"%s\"}", globals.ADMIN_PORTAL_USER, globals.ADMIN_PORTAL_PASSWORD,
		))),
	)
	body, _ := ioutil.ReadAll(resp.Body)
	var tokens map[string]interface{}
	json.Unmarshal(body, &tokens)

	login_token = tokens["access"].(string)
}

func make_request(url string) []byte {
	var resp *http.Response

	for {
		req, _ := http.NewRequest(
			"GET",
			url,
			nil,
		)
		req.Header.Add("Authorization", "Bearer "+login_token)
		var err error
		resp, err = client.Do(req)
		if err == nil {
			if resp.StatusCode == 401 {
				get_new_login_token()
			} else {
				break
			}
		}
	}

	body, _ := ioutil.ReadAll(resp.Body)

	return body
}

func get_community_of_database(database_identifier string) int {
	body := make_request(
		fmt.Sprintf(
			"%s/databases/?unique_identifier=%s", globals.ADMIN_PORTAL_URL, database_identifier,
		),
	)

	var database []map[string]interface{}
	json.Unmarshal(body, &database)

	if len(database) == 0 {
		return -1
	}
	return int(database[0]["community"].(float64))
}

func get_active_filters() []shared_structs.Filter {
	body := make_request(fmt.Sprintf("%s/filters/?status=ACTIVE", globals.ADMIN_PORTAL_URL))

	var filters []shared_structs.Filter
	json.Unmarshal(body, &filters)

	return filters
}
