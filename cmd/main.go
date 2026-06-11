package main

// Minimal sprbus CLI for debugging the event bus.
//
//	sprbus [-socket path]                 subscribe to all topics, print events
//	sprbus [-socket path] sub [prefix]    subscribe to a topic prefix
//	sprbus [-socket path] pub topic json  publish an event
import (
	"flag"
	"fmt"
	"log"
	"os"

	sprbus "github.com/spr-networks/sprbus-json"
)

func main() {
	socket := flag.String("socket", sprbus.ServerEventSock, "path to event bus unix socket")
	flag.Parse()
	args := flag.Args()

	cmd := "sub"
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}

	client, err := sprbus.NewClient(*socket)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	switch cmd {
	case "pub":
		if len(args) != 2 {
			log.Fatal("usage: sprbus pub <topic> <json-value>")
		}
		if _, err := client.Publish(args[0], args[1]); err != nil {
			log.Fatal(err)
		}
	case "sub":
		prefix := ""
		if len(args) > 0 {
			prefix = args[0]
		}
		stream, err := client.SubscribeTopic(prefix)
		if err != nil {
			log.Fatal(err)
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s %s\n", msg.GetTopic(), msg.GetValue())
		}
	default:
		fmt.Fprintln(os.Stderr, "usage: sprbus [-socket path] [sub [prefix] | pub <topic> <json>]")
		os.Exit(2)
	}
}
