package websocket

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/takebayashi/go-pulsar"
	"golang.org/x/net/websocket"
	"io"
)

type asyncProducer struct {
	ws *websocket.Conn
	ch chan *pulsar.Message
}

type producerMessage struct {
	Payload             string            `json:"payload"`
	Properties          map[string]string `json:"properties,omitempty"`
	Context             *string           `json:"context,omitempty"`
	Key                 *string           `json:"key,omitempty"`
	ReplicationClusters []string          `json:"replicationClusters,omitempty"`
}

type producerAckMessage struct {
	Result    string  `json:"result"`
	MessageId *string `json:"messageId"`
	ErrorMsg  *string `json:"errorMsg"`
	Context   *string `json:"context"`
}

func NewAsyncProducer(u string) (pulsar.AsyncProducer, error) {
	ws, err := websocket.Dial(u, "", "http://localhost")
	if err != nil {
		return nil, err
	}
	ch := make(chan *pulsar.Message)
	p := &asyncProducer{ws: ws, ch: ch}
	go p.listenAcks()
	go p.listenInputs()
	return p, nil
}

func (p *asyncProducer) Input() chan<- *pulsar.Message {
	return p.ch
}

func (p *asyncProducer) listenAcks() {
	d := json.NewDecoder(p.ws)
	for {
		ack := producerAckMessage{}
		if err := d.Decode(&ack); err == io.EOF {
			return
		} else if err != nil {
			p.handleError(err)
		} else if ack.Result != "ok" {
			p.handleError(errors.New(*ack.ErrorMsg))
		} else {
			p.handleSuccess(ack)
		}
	}
}

func (p *asyncProducer) listenInputs() {
	for m := range p.ch {
		d, err := json.Marshal(producerMessage{
			Payload:    base64.StdEncoding.EncodeToString(m.Data),
			Properties: m.Properties,
			Key:        &m.Key,
		})
		if err != nil {
			p.handleError(err)
			continue
		}
		d = append(d, byte('\n'))
		if _, err := p.ws.Write(d); err != nil {
			p.handleError(err)
		}
	}
}

func (p *asyncProducer) handleSuccess(m producerAckMessage) {
	// unimplemented
}

func (p *asyncProducer) handleError(e error) {
	// unimplemented
}
