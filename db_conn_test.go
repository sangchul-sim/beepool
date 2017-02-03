package beepool

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestConn(t *testing.T) {
	configStr := `{
        "default":{
            "address":"127.0.0.1",
            "charset":"utf8",
            "connection":"2",
            "db_name":"my_db",
            "driver":"mysql",
            "id":"my_account",
            "loc":"Asia%2FSeoul",
            "port":"3306",
            "pw":"my_password"
        },
        "slave":{
            "address":"127.0.0.1",
            "charset":"utf8",
            "connection":"3",
            "db_name":"my_db",
            "driver":"mysql",
            "id":"my_account",
            "loc":"Asia%2FSeoul",
            "port":"3306",
            "pw":"my_password"
        }
    }`

	dbconn := LoadConnectorJson(configStr)
	if err := dbconn.Connect(); err != nil {
		t.Errorf("got error Connect() %s%s", err)
	}

	pool := Pool()
	if b, err := json.Marshal(pool.ToMap()); err == nil {
		fmt.Println(string(b))
	} else {
		fmt.Println(err)
	}
}
