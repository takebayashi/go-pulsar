package pulsar

// AsyncConsumer consumes Pulsar messages asynchronously.
type AsyncConsumer interface {
	Messages() <-chan *Message
	Ack(*Message) error
}
