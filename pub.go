package natsevent

import jsoniter "github.com/json-iterator/go"

func Publish(eventName string, event any, opts ...ApplyPubOptsFunc) error {
	var pubOpts PublishOptions
	for _, opt := range opts {
		opt(&pubOpts)
	}
	if pubOpts.ConnName == "" {
		pubOpts.ConnName = ConnNameDefault
	}

	conn, ok := GetConn(pubOpts.ConnName)
	if !ok {
		return ErrNatsConnNotFound
	}

	data, err := jsoniter.Marshal(event)
	if err != nil {
		return err
	}

	if !IsConnEnabledJetStream(pubOpts.ConnName) {
		return conn.Publish(eventName, data)
	}

	js, err := GetJetStream(pubOpts.ConnName, eventName)
	if err != nil {
		return err
	}

	_, err = js.Publish(eventName, data)
	if err != nil {
		return err
	}

	return nil
}

type PublishOptions struct {
	ConnName string
}

type ApplyPubOptsFunc func(opt *PublishOptions)

func WithPubConnName(connName string) ApplyPubOptsFunc {
	return func(opt *PublishOptions) {
		opt.ConnName = connName
	}
}
