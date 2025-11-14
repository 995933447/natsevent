package natsevent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

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
	IsListenBroadcast        bool          // 是否广播方式监听
	IsIntoGlobalConsumerPool bool          // 是否进入全局消费者池中消费
	ConsumerPoolSize         uint64        // 全局消费者池大小
	ConnName                 string        // nats连接名称
	BatchSize                int           // 批量消费的最大数量
	MaxWait                  time.Duration // 批量消费最大等待消息时间
	ConsumeFastest           bool          // 批量消费的时候，是否尽快消费。为true的时候只要一有事件发布就会立刻消费，不会等待BatchSize
	MsgMaxRetry              uint64        // 消息消费失败最大重试次数
	MsgRetryInterval         time.Duration // 消息消重试的事件间隔
	MaxAckWait               time.Duration // 消息最长消费时间
	IdleHeartbeat            time.Duration // 消息心跳时间，用于检车消费者是否存活
	EnabledFlowControl       bool          // 是否开启流空机制
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

func WithBatchSize(batchSize int) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.BatchSize = batchSize
	}
}

func WithMaxWait(maxWait time.Duration) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.MaxWait = maxWait
	}
}

func WithMsgMaxRetry(msgMaxRetry uint64) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.MsgMaxRetry = msgMaxRetry
	}
}

func WithMsgRetryInterval(msgRetryInterval time.Duration) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.MsgRetryInterval = msgRetryInterval
	}
}

func WithMaxAckWait(maxAckWait time.Duration) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.MaxAckWait = maxAckWait
	}
}

func WithIdleHeartbeat(idleHeartbeat time.Duration) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.IdleHeartbeat = idleHeartbeat
	}
}

func WithEnableFlowControl() ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.EnabledFlowControl = true
	}
}

func WithConsumeFastest(consumeFastest bool) ApplySubOptsFunc {
	return func(opt *SubscribeOptions) {
		opt.ConsumeFastest = consumeFastest
	}
}

// Subscribe 这里如果启用jet stream并WithListenBroadcast()可能会收到重复投递的消息，
// 这是因为这里使用的是非Durable的临时消费者
// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
// 临时消费者没有持久化消费位置（last acked sequence）。
// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
// 程序需要做好幂等性
func Subscribe[T any](eventName, subscriberName string, handleFunc func(evt *T) error, opts ...ApplySubOptsFunc) error {
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

	var err error

	defer func() {
		if err != nil && localConsumerMsgCh != nil {
			close(localConsumerMsgCh)
		}
	}()

	if !enabledJs {
		if subOpts.IsListenBroadcast {
			_, err = conn.Subscribe(eventName, func(msg *nats.Msg) {
				callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
			})
			if err != nil {
				return err
			}

			return nil
		}

		_, err = conn.QueueSubscribe(eventName, normalizeNatsConsumerName(eventName+"_"+subscriberName), func(msg *nats.Msg) {
			callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
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

	var natsOpts []nats.SubOpt
	if subOpts.MaxAckWait > 0 {
		natsOpts = append(natsOpts, nats.AckWait(subOpts.MaxAckWait))
	}
	if subOpts.IdleHeartbeat > 0 {
		subOpts.EnabledFlowControl = true // IdleHeartbeat依赖EnableFlowControl实现 必须同时开启
		natsOpts = append(natsOpts, nats.IdleHeartbeat(subOpts.IdleHeartbeat))
	}
	if subOpts.EnabledFlowControl {
		natsOpts = append(natsOpts, nats.EnableFlowControl())
	}
	natsOpts = append(natsOpts, nats.ManualAck())

	if subOpts.IsListenBroadcast {
		// 这里可能会收到重复投递的消息，这是因为这里使用的是非Durable的临时消费者
		// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
		// 临时消费者没有持久化消费位置（last acked sequence）。
		// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
		// 程序需要做好幂等性
		_, err = js.Subscribe(eventName, func(msg *nats.Msg) {
			callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
		}, natsOpts...)
		if err != nil {
			return err
		}
		return nil
	}

	_, err = js.QueueSubscribe(eventName, normalizeNatsConsumerName(eventName+"_"+subscriberName), func(msg *nats.Msg) {
		callConsumer(msg, handleFunc, enabledJs, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
	}, natsOpts...)
	if err != nil {
		return err
	}

	return nil
}

func normalizeNatsConsumerName(name string) string {
	return strings.ReplaceAll(name, ".", "_")
}

func callConsumer[T any](msg *nats.Msg, handleFunc func(*T) error, isMenuAck bool, isIntoGlobalConsumerPool bool, localConsumerMsgCh chan *nats.Msg, msgMaxRetry uint64, msgRetryInterval time.Duration) {
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
					nakMsg(msg, msgMaxRetry, msgRetryInterval)
				}
				return err
			}

			if err := handleFunc(&event); err != nil {
				if isMenuAck {
					nakMsg(msg, msgMaxRetry, msgRetryInterval)
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

func nakMsg(msg *nats.Msg, msgMaxRetry uint64, msgRetryInterval time.Duration) {
	if msgMaxRetry > 0 {
		if deliveryCount, err := msg.Metadata(); err == nil && deliveryCount.NumDelivered > msgMaxRetry {
			err := msg.Term() // 放弃消息或写入死信队列
			if err != nil {
				LogErrorf("term fail: %v", runtimeutil.NewStackErr(err))
			}
			return
		}
	}

	if msgRetryInterval > 0 {
		err := msg.NakWithDelay(msgRetryInterval)
		if err != nil {
			LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
		}
		return
	}

	if err := msg.Nak(); err != nil {
		LogErrorf("nats msg nak error: %v", runtimeutil.NewStackErr(err))
	}
}

// SubscribeBroadcast 这里如果启用jet stream可能会收到重复投递的消息，
// 这是因为这里使用的是非Durable的临时消费者
// 每次运行订阅代码时，JetStream会认为这是没有标识的一个新的临时消费者。
// 临时消费者没有持久化消费位置（last acked sequence）。
// 所以它会把队列里未被ack的消息重新投递过来，即使是上一次运行进程已经确认的消息
// 程序需要做好幂等性
func SubscribeBroadcast[T any](eventName string, handleFunc func(evt *T) error, opts ...ApplySubOptsFunc) error {
	return Subscribe(eventName, "", handleFunc, append(opts, WithListenBroadcast())...)
}

func runLocalConsumerPool[T any](poolSize uint64, isMenuAck bool, handleFunc func(*T) error) chan *nats.Msg {
	ch := make(chan *nats.Msg)
	for i := uint64(0); i < poolSize; i++ {
		go func() {
			for {
				msg, ok := <-ch
				if !ok {
					return
				}

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

func SubscribeStreamBatch[T any](eventName, subscriberName string, handleFunc func(evts []*T) error, opts ...ApplySubOptsFunc) error {
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
	if subOpts.BatchSize <= 0 {
		subOpts.BatchSize = 100
	}
	if subOpts.MaxWait <= 0 {
		subOpts.MaxWait = time.Second * 10
	}

	enabledJs := IsConnEnabledJetStream(subOpts.ConnName)
	if !enabledJs {
		return ErrStreamNotFound
	}

	var localConsumerMsgCh chan []*nats.Msg
	if !subOpts.IsIntoGlobalConsumerPool {
		localConsumerMsgCh = runLocalBatchStreamConsumerPool(subOpts.ConsumerPoolSize, handleFunc, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
	}

	var err error

	defer func() {
		if err != nil && localConsumerMsgCh != nil {
			close(localConsumerMsgCh)
		}
	}()

	stream, err := GetJetStream(subOpts.ConnName, eventName)
	if err != nil {
		return err
	}

	sub, err := stream.PullSubscribe(eventName, normalizeNatsConsumerName(eventName+"_"+subscriberName))
	if err != nil {
		return err
	}

	go func() {
		defer sub.Unsubscribe()

		for {
			var messages []*nats.Msg
			start := time.Now()
			for len(messages) < subOpts.BatchSize {
				timeout := subOpts.MaxWait - time.Since(start)
				if timeout <= 0 {
					break
				}

				batch, err := sub.Fetch(subOpts.BatchSize-len(messages), nats.MaxWait(timeout))
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) {
						continue
					} else {
						LogErrorf("event:%s subscribe:%s fetch batch stream err:%+v", eventName, subscriberName, err)
						time.Sleep(time.Second * 5)
					}
				}

				if len(batch) > 0 {
					messages = append(messages, batch...)
				}

				if subOpts.ConsumeFastest {
					break
				}
			}

			if len(messages) > 0 {
				callBatchStreamConsumer(messages, handleFunc, subOpts.IsIntoGlobalConsumerPool, localConsumerMsgCh, subOpts.MsgMaxRetry, subOpts.MsgRetryInterval)
			}
		}
	}()

	return nil
}

func runLocalBatchStreamConsumerPool[T any](poolSize uint64, handleFunc func([]*T) error, msgMaxRetry uint64, msgRetryInterval time.Duration) chan []*nats.Msg {
	ch := make(chan []*nats.Msg)
	for i := uint64(0); i < poolSize; i++ {
		go func() {
			for {
				messages, ok := <-ch
				if !ok {
					return
				}
				_ = handleStreamBatch(messages, handleFunc, msgMaxRetry, msgRetryInterval)
			}
		}()
	}
	return ch
}

func callBatchStreamConsumer[T any](messages []*nats.Msg, handleFunc func([]*T) error, isIntoGlobalConsumerPool bool, localConsumerMsgCh chan []*nats.Msg, msgMaxRetry uint64, msgRetryInterval time.Duration) {
	if !isIntoGlobalConsumerPool {
		localConsumerMsgCh <- messages
		return
	}

	globalConsumerPoolMsgCh <- &globalConsumerMsg{
		handleFunc: func() error {
			return handleStreamBatch(messages, handleFunc, msgMaxRetry, msgRetryInterval)
		},
	}
}

func handleStreamBatch[T any](messages []*nats.Msg, handleFunc func([]*T) error, msgMaxRetry uint64, msgRetryInterval time.Duration) error {
	var (
		events        []*T
		validMessages []*nats.Msg
	)
	for _, msg := range messages {
		event := new(T)
		if err := jsoniter.Unmarshal(msg.Data, event); err != nil {
			LogErrorf("unmarshal nats msg fail: %v", runtimeutil.NewStackErr(err))
			nakMsg(msg, msgMaxRetry, msgRetryInterval)
			continue
		}
		events = append(events, event)
		validMessages = append(validMessages, msg)
	}

	if len(events) == 0 {
		return nil
	}

	if err := handleFunc(events); err != nil {
		for _, msg := range validMessages {
			nakMsg(msg, msgMaxRetry, msgRetryInterval)
		}
		return err
	}

	for _, msg := range validMessages {
		err := msg.Ack()
		if err != nil {
			LogErrorf("nats msg ack error: %v", runtimeutil.NewStackErr(err))
		}
	}

	return nil
}
