package model

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io"
	"log"
	"os"
	"strings"
	"flag"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "test", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	routingKey   = flag.String("key", "test-key", "AMQP routing key")
	body         = flag.String("body", "foobar", "Body of message")
	reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
)

func init() {
	flag.Parse()
}



func Loop(csvr *csv.Reader) {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("%s: %s", "Failed to connect to RabbitMQ", err)
	}
	defer conn.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		fmt.Printf("Channel: %s", err)
		os.Exit(1)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", *exchangeType, *exchangeName)
	if err := channel.ExchangeDeclare(
		*exchangeName,     // name
		*exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		fmt.Printf("Exchange Declare: %s", err)
		os.Exit(1)
	}

	if *reliable {
		log.Printf("enabling publishing confirms.")
		if err := channel.Confirm(false); err != nil {
			fmt.Printf("Channel could not be put into confirm mode: %s", err)
			os.Exit(1)
		}

		confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

		defer confirmOne(confirms)
	}

	log.Printf("declared Exchange, publishing %dB body (%q)", len(*body), *body)

	var enqueued, errors int
	go func() {
		for {

			row, err := csvr.Read()
			if err != nil {
				if err == io.EOF {
					fmt.Println("EOF reached, no more records to process")
					os.Exit(0)
				} else {
					fmt.Println("Error occored and cannot process anymore entry", err.Error())
					panic(err)
				}
			}

			msg_json := Message{Index: enqueued, Row: strings.Join(row[:], ",")}
			j, err := json.Marshal(msg_json)
			//
			if err != nil {
				fmt.Printf("Could not create the json representation of message %s", msg_json)
				panic(err)
			}

			if err = channel.Publish(
				*exchangeName,   // publish to an exchange
				*routingKey, // routing to 0 or more queues
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "application/json",
					ContentEncoding: "",
					Body:            j,
					DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
					Priority:        0,              // 0-9
					// a bunch of application/implementation-specific fields
				},
			); err != nil {
				fmt.Printf("Exchange Publish: %s", err)
				os.Exit(1)
			}
			println("Published message, yay!")
		}
	}()

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Printf("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf("confirmed delivery with delivery tag: %d", confirmed.DeliveryTag)
	} else {
		log.Printf("failed delivery of delivery tag: %d", confirmed.DeliveryTag)
	}
}

