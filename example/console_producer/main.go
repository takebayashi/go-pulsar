package main

import (
	"bufio"
	"flag"
	"github.com/takebayashi/go-pulsar"
	"github.com/takebayashi/go-pulsar/websocket"
	"os"
)

func main() {
	var u string
	flag.StringVar(&u, "u", "", "Pulsar WebSocket URL")
	flag.Parse()
	c, err := websocket.NewAsyncProducer(u)
	if err != nil {
		panic(err)
	}
	sc := bufio.NewScanner(os.Stdin)
	for sc.Scan() {
		c.Input() <- &pulsar.Message{Data: sc.Bytes()}
	}
}
