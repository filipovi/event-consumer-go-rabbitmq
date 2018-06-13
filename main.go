package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"path/filepath"

	"github.com/filipovi/elastic"
	"github.com/filipovi/rabbitmq"
	"goji.io/pat"
)

/**
 * @TODO
 */

// Env contains the ElasticSearch client
type Env struct {
	client  elastic.Client
	channel rabbitmq.Channel
}

// HANDLERS

func (env *Env) handleHomeRequest(w http.ResponseWriter, req *http.Request) {
	send([]byte("The service mini-search-engine is working!"), "text/plain", http.StatusOK, w)
}

func (env *Env) handleSearchRequest(w http.ResponseWriter, req *http.Request) {
	term := pat.Param(req, "term")
	res, err := env.client.NewSearchQuery(term, 0, 20)
	if err != nil {
		log.Printf("ERROR: [Request] %s", err)
		send([]byte(err.Error()), "text/plain", http.StatusBadRequest, w)

		return
	}
	doc, _ := json.Marshal(res)
	send([]byte(doc), "application/json", http.StatusOK, w)
}

// MAIN

func send(content []byte, contentType string, status int, w http.ResponseWriter) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%v", len(content)))
	w.WriteHeader(status)
	w.Write(content)
}

func failOnError(err error, msg string) {
	if err == nil {
		return
	}
	log.Fatalf("%s: %s", msg, err)
	panic(fmt.Sprintf("%s: %s", msg, err))
}

func connect(file string) (*Env, error) {
	path, err := filepath.Abs(file)
	if err != nil {
		log.Fatal(err)
	}
	elastic, err := elastic.New(path)
	if nil != err {
		return nil, err
	}
	log.Println("Elastic connected!")

	rabbitmq, err := rabbitmq.New(path)
	if nil != err {
		return nil, err
	}
	log.Println("Rabbitmq connected!")

	env := &Env{
		client:  *elastic,
		channel: *rabbitmq,
	}

	return env, nil
}

func main() {
	env, err := connect("config.json")
	failOnError(err, "Failed to connect to ES")

	err = env.channel.NewExchange("event_message.mailchimp")
	failOnError(err, "Failed to create an apmq exchange")

	q, err := env.channel.NewQueue("")
	failOnError(err, "Failed to declare a queue")

	err = env.channel.BindQueue(q.Name, "event_message.mailchimp")
	failOnError(err, "Failed to bind a queue")

	msgs, err := env.channel.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
