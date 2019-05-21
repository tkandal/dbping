package dbping

import (
	"fmt"
	bolt "github.com/tkandal/golang-neo4j-bolt-driver"
	"io"
	"time"
)

type neo4JPing struct {
	uri string
}

// NewNeo4JPing returns a db pinger for Neo4J
func NewNeo4JPing(uri string) Pinger {
	return &neo4JPing{uri: uri}
}

// Ping runs the given query with optional parameters and return nil if successful,- or an error otherwise.
// The given query must be a legal query in order to give a correct result.  The returned error will indicate what
// is incorrect in case the query is illegal.
// Also care should be taken so that the query does not return too many rows,- it is after all, just to see
// if the database is available.
func (np *neo4JPing) Ping(query string, params map[string]interface{}) error {
	conn, err := createConnection(np.uri)
	if err != nil {
		return fmt.Errorf("create connection failed; error = %v", err)
	}
	defer conn.Close()

	stmt, err := conn.PrepareNeo(query)
	if err != nil {
		return fmt.Errorf("prepare query failed; error = %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.QueryNeo(params)
	if err != nil {
		return fmt.Errorf("execute query failed; error = %v", err)
	}
	defer rows.Close()

	for {
		_, _, err = rows.NextNeo()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("next row failed; error = %v", err)
		}
	}
	return nil
}

func createConnection(neo4jURI string) (bolt.Conn, error) {
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo(neo4jURI)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		tries := 1
		for err != nil && tries < 3 {
			conn, err = driver.OpenNeo(neo4jURI)
			time.Sleep(time.Duration(tries) * time.Second)
			tries++
		}
	}
	return conn, err
}
