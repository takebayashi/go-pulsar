package pulsar

// AsyncProducer produces Pulsar messages asynchronously.
type AsyncProducer interface {
	Input() chan<- *Message
}
