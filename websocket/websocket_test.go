package websocket_test

import (
	"fmt"
	"github.com/takebayashi/go-pulsar"
	"github.com/takebayashi/go-pulsar/websocket"
	"os"
	"testing"
	"time"
)

func TestWebSocket(t *testing.T) {
	n := 10
	up := os.Getenv("WS_PRODUCE_URI")
	uc := os.Getenv("WS_CONSUME_URI")
	if up == "" || uc == "" {
		t.Skip("WS_PRODUCE_URI or WS_CONSUME_URI is not set")
	}
	p, err := websocket.NewAsyncProducer(up)
	if err != nil {
		t.Error(err)
	}
	c, err := websocket.NewAsyncConsumer(uc)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < n; i++ {
		tx := fmt.Sprintf("msg#%d", i)
		p.Input() <- &pulsar.Message{Data: []byte(tx)}
	}
	for i := 0; i < n; i++ {
		select {
		case m := <-c.Messages():
			tx := fmt.Sprintf("msg#%d", i)
			if string(m.Data) != tx {
				t.Error("message should by '%s', but '%s'", tx, string(m.Data))
			}
		case <-time.After(1 * time.Second):
			t.Error("consumer timed out")
		}
	}
}
