package natsevent

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const ConnNameDefault = "default"

var connMap sync.Map

func SetConn(name string, conn *nats.Conn) {
	connMap.Store(name, conn)
}

func GetConn(name string) (*nats.Conn, bool) {
	conn, ok := connMap.Load(name)
	return conn.(*nats.Conn), ok
}

func CloseConn(name string) error {
	conn, ok := GetConn(name)
	if !ok {
		return nil
	}
	connMap.Delete(name)
	err := conn.Drain()
	if err != nil {
		return err
	}
	return nil
}

type ConnConfig struct {
	User     string
	Password string
	Timeout  time.Duration
	Secure   bool
	RootCa   string
	Servers  []string
}

func Connect(name string, cfg *ConnConfig) (*nats.Conn, error) {
	var tlsCfg *tls.Config
	if cfg.Secure {
		tlsCfg = &tls.Config{}
		tlsCfg.InsecureSkipVerify = true
		if cfg.RootCa != "" {
			rootCAs := x509.NewCertPool()
			loaded, err := os.ReadFile(cfg.RootCa)
			if err != nil {
				return nil, fmt.Errorf("unexpected missing certfile: %v", err)
			}
			rootCAs.AppendCertsFromPEM(loaded)
			tlsCfg.RootCAs = rootCAs
		}
	}

	var opts = nats.Options{
		User:           cfg.User,
		Password:       cfg.Password,
		Servers:        cfg.Servers,
		Name:           name,
		AllowReconnect: true,
		MaxReconnect:   -1,
		ReconnectWait:  100 * time.Millisecond,
		Timeout:        cfg.Timeout,
		Secure:         cfg.Secure,
		TLSConfig:      tlsCfg,
	}

	conn, err := opts.Connect()
	if err != nil {
		return nil, err
	}

	SetConn(name, conn)

	return conn, nil
}

func ConnectDefault(cfg *ConnConfig) (*nats.Conn, error) {
	return Connect(ConnNameDefault, cfg)
}

func GetDefaultConn() (*nats.Conn, bool) {
	return GetConn(ConnNameDefault)
}

func SetDefaultConn(conn *nats.Conn) {
	SetConn(ConnNameDefault, conn)
}

func CloseDefaultConn() error {
	return CloseConn(ConnNameDefault)
}
