package natsevent

import (
	"sync"

	"github.com/995933447/runtimeutil"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
)

type globalConsumerMsg struct {
	handleFunc func() error
}

var (
	globalConsumerPoolMsgCh       = make(chan *globalConsumerMsg)
	initGlobalConsumerPoolOnce    sync.Once
	initializedGlobalConsumerPool bool
)

func InitGlobalConsumerPool(poolSize uint64) {
	initGlobalConsumerPoolOnce.Do(func() {
		for i := uint64(0); i < poolSize; i++ {
			go func() {
				for {
					msg := <-globalConsumerPoolMsgCh
					_ = msg.handleFunc()
				}
			}()
		}
		initializedGlobalConsumerPool = true
	})
}

type SubscribeOptions struct {
	IsListenBroadcast        bool
	IsIntoGlobalConsumerPool bool
	ConsumerPoolSize         uint64
	ConnName                 string
}

type ApplySubOptsFunc func(opt *SubscribeOptions)

func WithListenBroadcast() ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.IsListenBroadcast = true
	}
}

func WithConnName(connName string) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.ConnName = connName
	}
}

func WithIntoGlobalConsumerPool() ApplySubOptsFunc {
	return func(opts *SubscribeOptions) {
		opts.IsIntoGlobalConsumerPool = true
	}
}

func WithConsumerPoolSize(consumerPoolSize uint64) ApplySubOptsFunc {
	return func(opts *SubscribeOptions) {
		opts.ConsumerPoolSize = consumerPoolSize
	}
}

// Subscribe 这里如果启用jet stream并WithListenBroadcast()可能会收到重复投递的消息，
// 这是因为这里使用的是非Durable的临时消费者
// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
// 临时消费者没有持久化消费位置（last acked sequence）。
// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
// 程序需要做好幂等性
func Subscribe[T any](eventName, subscriberName string, handleFunc func(any *T) error, opts ...ApplySubOptsFunc) error {
	var subOpts SubscribeOptions
	for _, opt := range opts {
		opt(&subOpts)
	}
	if subOpts.ConsumerPoolSize <= 0 {
		subOpts.ConsumerPoolSize = 1
	}
	if subOpts.ConnName == "" {
		subOpts.ConnName = ConnNameDefault
	}
	if subOpts.IsListenBroadcast {
		subOpts.ConsumerPoolSize = 1
		subOpts.IsIntoGlobalConsumerPool = false
	}
	if subOpts.IsIntoGlobalConsumerPool && !initializedGlobalConsumerPool {
		return ErrGlobalConsumerNotInitialized
	}

	conn, ok := GetConn(subOpts.ConnName)
	if !ok {
		return ErrNatsConnNotFound
	}

	enabledJs := IsConnEnabledJetStream(subOpts.ConnName)

	var localConsumerMsgCh chan *nats.Msg
	if !subOpts.IsIntoGlobalConsumerPool {
		localConsumerMsgCh = runLocalConsumerPool(subOpts.ConsumerPoolSize, enabledJs, handleFunc)
	}

	if !enabledJs {
		if subOpts.IsListenBroadcast {
			_, err := conn.Subscribe(eventName, func(msg *nats.Msg) {
				callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh)
			})
			if err != nil {
				return err
			}

			return nil
		}

		_, err := conn.QueueSubscribe(eventName, subscriberName, func(msg *nats.Msg) {
			callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh)
		})
		if err != nil {
			return err
		}

		return nil
	}

	js, err := GetJetStream(subOpts.ConnName, eventName)
	if err != nil {
		return err
	}

	if subOpts.IsListenBroadcast {
		// 这里可能会收到重复投递的消息，这是因为这里使用的是非Durable的临时消费者
		// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
		// 临时消费者没有持久化消费位置（last acked sequence）。
		// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
		// 程序需要做好幂等性
		_, err = js.Subscribe(eventName, func(msg *nats.Msg) {
			callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh)
		}, nats.ManualAck())
		if err != nil {
			return err
		}
		return nil
	}

	_, err = js.QueueSubscribe(eventName, subscriberName, func(msg *nats.Msg) {
		callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh)
	}, nats.ManualAck())
	if err != nil {
		return err
	}

	return nil
}

func callConsumer[T any](msg *nats.Msg, handleFunc func(*T) error, isMenuAck bool, isIntoGlobalConsumerPool bool, localConsumerMsgCh chan *nats.Msg) {
	if !isIntoGlobalConsumerPool {
		localConsumerMsgCh <- msg
		return
	}

	globalConsumerPoolMsgCh <- &globalConsumerMsg{
		handleFunc: func() error {
			var event T
			if err := jsoniter.Unmarshal(msg.Data, &event); err != nil {
				LogErrorf("unmarshal nats msg fail: %v", runtimeutil.NewStackErr(err))
				if isMenuAck {
					if err := msg.Nak(); err != nil {
						LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
					}
				}
				return err
			}
			if err := handleFunc(&event); err != nil {
				if isMenuAck {
					if err := msg.Nak(); err != nil {
						LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
					}
				}
				return err
			}
			if isMenuAck {
				if err := msg.Ack(); err != nil {
					LogErrorf("nats msg ack error: %v", runtimeutil.NewStackErr(err))
				}
			}
			return nil
		},
	}
}

// SubscribeBroadcast 这里如果启用jet stream可能会收到重复投递的消息，
// 这是因为这里使用的是非Durable的临时消费者
// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
// 临时消费者没有持久化消费位置（last acked sequence）。
// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
// 程序需要做好幂等性
func SubscribeBroadcast[T any](eventName, subscriberName string, handleFunc func(any *T) error, opts ...ApplySubOptsFunc) error {
	return Subscribe(eventName, subscriberName, handleFunc, append(opts, WithListenBroadcast())...)
}

func runLocalConsumerPool[T any](poolSize uint64, isMenuAck bool, handleFunc func(*T) error) chan *nats.Msg {
	ch := make(chan *nats.Msg)
	for i := uint64(0); i < poolSize; i++ {
		go func() {
			for {
				msg := <-ch
				var event T
				if err := jsoniter.Unmarshal(msg.Data, &event); err != nil {
					LogErrorf("unmarshal nats msg fail: %v", runtimeutil.NewStackErr(err))
					if isMenuAck {
						err = msg.Nak()
						if err != nil {
							LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
						}
					}
					continue
				}
				if err := handleFunc(&event); err != nil {
					if isMenuAck {
						err = msg.Nak()
						if err != nil {
							LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
						}
					}
					continue
				}
				if isMenuAck {
					err := msg.Ack()
					if err != nil {
						LogErrorf("nats msg ack error: %v", runtimeutil.NewStackErr(err))
					}
				}
			}
		}()
	}
	return ch
}
