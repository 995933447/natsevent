package natsevent

import "errors"

var (
	ErrNatsConnNotFound             = errors.New("nats conn not found")
	ErrStreamNotFound               = errors.New("stream not found")
	ErrGlobalConsumerNotInitialized = errors.New("global consumer not initialized")
)
