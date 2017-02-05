package pulsar

import "time"

type Message struct {
	Properties map[string]string
	Data       []byte
	Id         string
	Key        string
	Timestamp  time.Time
}
