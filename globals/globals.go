package globals

import (
	"fmt"
	"os"
	"strings"
)

var BOOTSTRAP_SERVERS []string
var FILTER_WORKER_ID int // TODO get this as a program argument OR have generate unique id mechanism
var ADMIN_PORTAL_URL string
var KSQLDB_URL string
var SCHEMA_REGISTRY_URL string

func Init_global_variables() {
	BOOTSTRAP_SERVERS = strings.Split(os.Getenv("BOOTSTRAP_SERVERS"), ",")

	ADMIN_PORTAL_BACKEND_HOST := os.Getenv("ADMIN_PORTAL_BACKEND_HOST")
	ADMIN_PORTAL_BACKEND_PORT := os.Getenv("ADMIN_PORTAL_BACKEND_PORT")
	ADMIN_PORTAL_URL = fmt.Sprintf("http://%s:%s/api", ADMIN_PORTAL_BACKEND_HOST, ADMIN_PORTAL_BACKEND_PORT)

	KSQLDB_HOST := os.Getenv("KSQLDB_HOST")
	KSQLDB_PORT := os.Getenv("KSQLDB_PORT")
	KSQLDB_URL = fmt.Sprintf("http://%s:%s", KSQLDB_HOST, KSQLDB_PORT)

	SCHEMA_REGISTRY_HOST := os.Getenv("SCHEMA_REGISTRY_HOST")
	SCHEMA_REGISTRY_PORT := os.Getenv("SCHEMA_REGISTRY_PORT")
	SCHEMA_REGISTRY_URL = fmt.Sprintf("http://%s:%s", SCHEMA_REGISTRY_HOST, SCHEMA_REGISTRY_PORT)
}
