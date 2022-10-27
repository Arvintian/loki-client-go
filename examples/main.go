package main

import (
	"log"
	"os"
	"time"

	"github.com/Arvintian/loki-client-go/loki"
)

func main() {
	hostname, _ := os.Hostname()

	lokiLogger, err := loki.NewWithDefault("http://localhost:3000/loki/api/v1/push")
	if err != nil {
		log.Fatal(err)
	}

	labels := map[string]string{
		"local_dev": hostname,
	}

	lokiLogger.Handle(labels, time.Now(), "line test 1")
	lokiLogger.Handle(labels, time.Now(), "line test 2")
	lokiLogger.Handle(labels, time.Now(), "line test 3")

	defer lokiLogger.Stop()
}
