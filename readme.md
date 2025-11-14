# natsevent 框架文档

natsevent 是基于 NATS 与 JetStream 封装的事件总线框架，提供简洁的事件发布和订阅接口，支持：

* 普通事件订阅
* 广播事件订阅
* 批量流式消费
* 局部消费者池
* 全局消费者池
* 可配置消费重试、批量大小、等待时间等参数

---

## 安装

确保已安装 NATS Server 并启动 JetStream。

```bash
# 使用 Docker 启动 NATS Server
docker run -p 4222:4222 -p 8222:8222 nats:latest -js
```

在 Go 项目中引入：

```go
import "github.com/995933447/natsevent"
```

---

## 核心概念

* **事件(Event)**：通过结构体定义，每个事件包含对应的消息内容。
* **主题(EventName)**：事件名称，用于发布和订阅。
* **消费者(Subscriber)**：事件的处理者，可以是单条消费或批量消费。
* **全局消费者池(Global Consumer Pool)**：统一管理多个订阅者的并发处理，提高资源利用率。
* **广播事件(Broadcast)**：所有订阅者都会收到同一条消息。

---

## 定义事件

```go
type LoginEvent struct {
    UserId   uint64
    NickName string
}

func (e *LoginEvent) Send() error {
    return Publish("user.login", e)
}

type RegisterEvent struct {
    UserId uint64
    Ts     int64
}

func (e *RegisterEvent) Send() error {
    return Publish("user.register", e)
}
```

---

## 连接配置

```go
conn, err := natsevent.ConnectDefault(&natsevent.ConnConfig{
    Servers:  []string{"nats://127.0.0.1:4222"},
    User:     "root",
    Password: "123456",
})
if err != nil {
    panic(err)
}
```

也可以为不同业务创建多个连接：

```go
_, err := natsevent.Connect("user", &natsevent.ConnConfig{
    Servers:  []string{"nats://127.0.0.1:4222"},
    User:     "root",
    Password: "123456",
})
```

或者设置应用里已有连接
```go
natsevent.SetConn("order", conn)
```
---

## 初始化 JetStream

```go
natsevent.InitJetStream("user", &nats.StreamConfig{
    MaxAge:   time.Hour,
    Replicas: 3,
})
```

* **MaxAge**：消息在流中的最大存活时间。
* **Replicas**：流的副本数。

---

## 发布事件

```go
login := &LoginEvent{UserId: 1001, NickName: "Alice"}
err := login.Send()
```

---

## 普通订阅

```go
natsevent.Subscribe("user.login", "subscriber1", func(e *LoginEvent) error {
    fmt.Println("Received login:", e)
    return nil
})
```

* **参数说明**：

    * `eventName`：事件名称
    * `subscriberName`：订阅者唯一标识,可以理解为一个独立的队列
    * `handleFunc`：处理函数

---

## 广播订阅

```go
natsevent.SubscribeBroadcast("user.register", "broadcastSubscriber", func(e *RegisterEvent) error {
    fmt.Println("Received broadcast register:", e)
    return nil
})
```

* 所有订阅者都会接收到同一条消息。

---

## 批量流式订阅

```go
natsevent.SubscribeStreamBatch("user.login", "batchSubscriber", func(events []*LoginEvent) error {
    fmt.Println("Batch events:", events)
    return nil
}, WithBatchSize(100), WithMaxWaitMsec(5000))
```

* **参数说明**：

    * `BatchSize`：每批处理的最大消息数
    * `MaxWaitMsec`：等待消息的最大时间（毫秒）
    * 支持 `WithIntoGlobalConsumerPool()` 将订阅加入全局池
    * 支持 `WithConsumeFastest(true)` 优先快速消费消息

---

## 全局消费者池

```go
natsevent.InitGlobalConsumerPool(5)
```

* 创建全局消费者池，统一处理加入池的订阅。为一参数是池子大小。

---

## 配置选项

| 选项                     | 说明            |
| ---------------------- | ------------- |
| `ConsumerPoolSize`     | 并发消费者数量       |
| `MsgMaxRetry`          | 消息处理失败最大重试次数  |
| `MsgRetryInterval`     | 重试间隔          |
| `ConnName`             | 使用的 NATS 连接名称 |
| `IsIntoGlobalConsumerPool` | 是否加入全局消费者池    |
| `BatchSize`            | 批量订阅每批消息数     |
| `MaxWaitMsec`          | 批量订阅等待时间      |
| `IsListenBroadcast`    | 是否广播订阅        |
| `ConsumeFastest`       | 批量消费的时候，是否尽快消费。为true的时候只要一有事件发布就会立刻消费，不会等待BatchSize      |

---

## 示例测试

```go
for i := 0; i < 10; i++ {
    (&LoginEvent{UserId: uint64(i)}).Send()
}

select {} // 阻塞主线程，等待事件处理
```

---

## 注意事项

1. **订阅者名称**应唯一，避免重复订阅。
2. **批量订阅**会等待 `BatchSize` 条消息或 `MaxWaitMsec` 超时后处理。
3. **全局消费者池**可以提升吞吐量，但需要先初始化。
4. 广播订阅不会加入全局池，每个订阅者都能收到消息。

---

