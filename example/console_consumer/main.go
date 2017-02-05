package main

import (
	"flag"
	"fmt"
	"github.com/takebayashi/go-pulsar/websocket"
)

func main() {
	var u string
	flag.StringVar(&u, "u", "", "Pulsar WebSocket URL")
	flag.Parse()
	c, err := websocket.NewAsyncConsumer(u)
	if err != nil {
		panic(err)
	}
	for m := range c.Messages() {
		fmt.Printf("%+v: %s\n", m.Timestamp, string(m.Data))
		c.Ack(m)
	}
}
