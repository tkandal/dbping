// Package dbping is kind of a convenience package to provide a pinger for database-drivers that don't implement
// a ping-method.
package dbping

// Pinger is the basic interface for pinging  an database with a custom query and closing it afterwards.
type Pinger interface {
	Ping(string, map[string]interface{}) error
}
