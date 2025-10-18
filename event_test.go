package natsevent

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type LoginEvent struct {
	UserId   uint64
	NickName string
}

const (
	LoginEventName    = "user.login"
	RegisterEventName = "user.register"
)

func (e *LoginEvent) Send() error {
	return Publish(LoginEventName, e)
}

type RegisterEvent struct {
	UserId uint64
	Ts     int64
}

func (e *RegisterEvent) Send() error {
	return Publish(RegisterEventName, e)
}

func TestEvent(t *testing.T) {
	conn, err := ConnectDefault(&ConnConfig{
		Servers:  []string{"nats://127.0.0.1:8111"},
		User:     "root",
		Password: "123456",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = Connect("user", &ConnConfig{
		Servers:  []string{"nats://127.0.0.1:8111"},
		User:     "root",
		Password: "123456",
	})
	if err != nil {
		t.Fatal(err)
	}

	SetConn("order", conn)

	InitGlobalConsumerPool(1)

	InitJetStream("user", &nats.StreamConfig{
		MaxAge:   time.Minute,
		Replicas: 3,
	})

	err = Subscribe(LoginEventName, "testing", func(e *LoginEvent) error {
		fmt.Println("event sub work1:", e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = Subscribe(LoginEventName, "testing", func(e *LoginEvent) error {
		fmt.Println("event sub work2:", e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = SubscribeBroadcast(LoginEventName, "testing", func(e *RegisterEvent) error {
		fmt.Println("event broadcast:", e)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = Subscribe(RegisterEventName, "testing", func(e *LoginEvent) error {
		fmt.Println("register event sub work1:", e)
		return nil
	}, WithConnName("user"), WithIntoGlobalConsumerPool())
	if err != nil {
		t.Fatal(err)
	}

	err = Subscribe(RegisterEventName, "testing", func(e *LoginEvent) error {
		fmt.Println("register event sub work2:", e)
		return nil
	}, WithConnName("user"), WithIntoGlobalConsumerPool())
	if err != nil {
		t.Fatal(err)
	}

	err = SubscribeBroadcast(RegisterEventName, "testing", func(e *RegisterEvent) error {
		fmt.Println("register event broadcast:", e)
		return nil
	}, WithConnName("user"))
	if err != nil {
		t.Fatal(err)
	}

	err = (&LoginEvent{UserId: 10010}).Send()
	if err != nil {
		t.Fatal(err)
	}

	err = (&RegisterEvent{UserId: 10010, Ts: time.Now().Unix()}).Send()
	if err != nil {
		t.Fatal(err)
	}

	select {}
}
