package dbping

import (
	"context"
	"fmt"
	bolt "github.com/tkandal/golang-neo4j-bolt-driver"
	"io"
	"time"
)

// Neo4JPing is pinger for a Neo4J database
type Neo4JPing struct {
	uri string
}

// NewNeo4JPing returns a database pinger for Neo4J
func NewNeo4JPing(uri string) *Neo4JPing {
	return &Neo4JPing{uri: uri}
}

// Ping runs the given query with optional parameters and return nil if successful,- or an error otherwise.
// The given query must be a legal query in order to give a correct result.  The returned error will indicate what
// is incorrect in case the query is illegal.
// Also care should be taken so that the query does not return too many rows,- it is after all, just to see
// if the database is available
func (np *Neo4JPing) Ping(ctx context.Context, query string, params map[string]interface{}) error {
	conn, err := createConnection(ctx, np.uri)
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
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, _, err := rows.NextNeo()
			if err == io.EOF || data == nil {
				return nil
			}
			if err != nil {
				return fmt.Errorf("next row failed; error = %v", err)
			}
		}
	}
}

func createConnection(ctx context.Context, neo4jURI string) (bolt.Conn, error) {
	driver := bolt.NewDriver()
	conn, err := driver.OpenNeo(neo4jURI)
	if err != nil {
		for tries := 1; tries < 3 && err != nil; tries++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				time.Sleep(time.Duration(tries) * time.Second)
				conn, err = driver.OpenNeo(neo4jURI)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return conn, err
}
