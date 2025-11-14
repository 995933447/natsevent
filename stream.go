package natsevent

import (
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
)

var (
	enabledJetStreamToAllConn bool
	globalStreamCfg           *nats.StreamConfig
)

var mapConnNameToStreamCfg = make(map[string]*nats.StreamConfig)

func InitGlobalJetStream(streamCfg *nats.StreamConfig) {
	enabledJetStreamToAllConn = true
	globalStreamCfg = streamCfg
}

func InitJetStream(connName string, streamCfg *nats.StreamConfig) {
	mapConnNameToStreamCfg[connName] = streamCfg
}

func InitDefaultConnJetStream(streamCfg *nats.StreamConfig) {
	InitJetStream(ConnNameDefault, streamCfg)
}

func IsConnEnabledJetStream(connName string) bool {
	if enabledJetStreamToAllConn {
		return true
	}

	return mapConnNameToStreamCfg[connName] != nil
}

var (
	mapConnNameToEvtNameToJs   = make(map[string]map[string]nats.JetStreamContext)
	mapConnNameToEvtNameToJsMu sync.RWMutex
	mapEvtNameToJsMu           sync.RWMutex
)

func GetJetStream(connName, eventName string) (nats.JetStreamContext, error) {
	if !IsConnEnabledJetStream(connName) {
		return nil, ErrStreamNotFound
	}

	mapConnNameToEvtNameToJsMu.RLock()
	mapEvtNameToJs, ok := mapConnNameToEvtNameToJs[connName]
	mapConnNameToEvtNameToJsMu.RUnlock()
	if ok {
		mapEvtNameToJsMu.RLock()
		js, ok := mapEvtNameToJs[eventName]
		mapEvtNameToJsMu.RUnlock()
		if ok {
			return js, nil
		}
	} else {
		mapConnNameToEvtNameToJsMu.Lock()
		mapEvtNameToJs, ok = mapConnNameToEvtNameToJs[connName]
		if !ok {
			mapEvtNameToJs = make(map[string]nats.JetStreamContext)
			mapConnNameToEvtNameToJs[connName] = mapEvtNameToJs
		}
		mapConnNameToEvtNameToJsMu.Unlock()
	}

	mapEvtNameToJsMu.Lock()
	defer mapEvtNameToJsMu.Unlock()

	js, ok := mapEvtNameToJs[eventName]

	var err error
	if !ok {
		conn, ok := GetConn(connName)
		if !ok {
			return nil, ErrStreamNotFound
		}

		js, err = conn.JetStream()
		if err != nil {
			return nil, err
		}

		streamName := connName
		evtNameComp := strings.Split(eventName, ".")
		if len(evtNameComp) >= 2 {
			streamName = evtNameComp[0]
		}
		streamInfo, err := js.StreamInfo(streamName)
		if err != nil {
			if errors.Is(err, nats.ErrStreamNotFound) {
				cfg, err := getJetStreamCfg(connName)
				if err != nil {
					return nil, err
				}

				cfg.Name = streamName
				cfg.Subjects = []string{eventName}
				_, err = js.AddStream(cfg)
				if err != nil {
					return nil, err
				}
			}
		} else {
			var exists bool
			for _, subject := range streamInfo.Config.Subjects {
				if subject == eventName {
					exists = true
					break
				}
			}
			if !exists {
				streamInfo.Config.Subjects = append(streamInfo.Config.Subjects, eventName)
				_, err = js.UpdateStream(&streamInfo.Config)
				if err != nil {
					return nil, err
				}
			}
		}
		mapEvtNameToJs[eventName] = js
	}

	return js, nil
}

func getJetStreamCfg(connName string) (*nats.StreamConfig, error) {
	if !IsConnEnabledJetStream(connName) {
		return nil, ErrStreamNotFound
	}

	cfg, ok := mapConnNameToStreamCfg[connName]
	if ok {
		return cfg, nil
	}

	if enabledJetStreamToAllConn {
		return globalStreamCfg, nil
	}

	return nil, ErrStreamNotFound
}
