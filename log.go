package sprbus

import (
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"strings"
)

type Hook struct {
	client    *Client
	prefix    string
	LogLevels []logrus.Level
}

func NewSprBusHook(prefix string) (*Hook, error) {
	client, err := NewClient(ServerEventSock)
	return &Hook{client, prefix, []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}}, err
}

func (hook *Hook) Fire(entry *logrus.Entry) error {
	line, err := entry.Bytes()
	if err != nil {
		return err
	}

	if hook.client == nil {
		os.Stderr.Write(line)
		return nil
	}

	_, err = hook.client.Publish(hook.prefix, strings.Trim(string(line), "\n"))
	if err != nil {
		os.Stderr.Write(line)
		return nil
	}

	return err
}

func (hook *Hook) Levels() []logrus.Level {
	return hook.LogLevels
}

func NewLog(prefix string) *logrus.Logger {
	if prefix == "" {
		prefix = "log"
	}

	log := logrus.New()
	log.SetOutput(ioutil.Discard)
	log.SetLevel(logrus.DebugLevel)
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetReportCaller(true)

	h, _ := NewSprBusHook(prefix)
	log.Hooks.Add(h)

	return log
}
