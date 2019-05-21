package dbping

// PingCloser is the basic interface for pinging  an database with a custom query and closing it afterwards.
type Pinger interface {
	Ping(string, map[string]interface{}) error
}

