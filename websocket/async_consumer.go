package websocket

import (
	"encoding/base64"
	"encoding/json"
	"github.com/takebayashi/go-pulsar"
	"golang.org/x/net/websocket"
	"io"
	"time"
)

type asyncConsumer struct {
	ws *websocket.Conn
	ch chan *pulsar.Message
}

type clientAckMessage struct {
	MessageId string `json:"messageId"`
}

type consumerMessage struct {
	MessageId   string            `json:"messageId"`
	Payload     string            `json:"payload"`
	Properties  map[string]string `json:"properties"`
	PublishTime string            `json:"publishTime"`
	Context     string            `json:"context"`
	Key         string            `json:"key"`
}

func NewAsyncConsumer(u string) (pulsar.AsyncConsumer, error) {
	ws, err := websocket.Dial(u, "", "http://localhost")
	if err != nil {
		return nil, err
	}
	ch := make(chan *pulsar.Message)
	c := &asyncConsumer{ws: ws, ch: ch}
	go c.consume()
	return c, nil
}

func (c *asyncConsumer) Messages() <-chan *pulsar.Message {
	return c.ch
}

func (c *asyncConsumer) Ack(m *pulsar.Message) error {
	ack, err := json.Marshal(clientAckMessage{MessageId: m.Id})
	if err != nil {
		return err
	}
	ack = append(ack, byte('\n'))
	_, err = c.ws.Write(ack)
	return err
}

func (c *asyncConsumer) consume() {
	d := json.NewDecoder(c.ws)
	for {
		cm := consumerMessage{}
		if err := d.Decode(&cm); err == io.EOF {
			return
		} else if err != nil {
			c.handleError(err)
		}
		d, err := base64.StdEncoding.DecodeString(cm.Payload)
		if err != nil {
			c.handleError(err)
		}
		tm, err := time.Parse("2006-01-02 15:04:05.999", cm.PublishTime)
		if err != nil {
			c.handleError(err)
		}
		m := pulsar.Message{
			Id:        cm.MessageId,
			Data:      d,
			Timestamp: tm,
		}
		c.ch <- &m
	}
}

func (c *asyncConsumer) handleError(e error) {
	// unimplemented
}
