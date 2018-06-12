package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/filipovi/event-populate/config"
	"github.com/filipovi/event-populate/elastic"
	"github.com/filipovi/event-populate/rabbitmq"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/rs/cors"
	"github.com/urfave/negroni"
	goji "goji.io"
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

func connect(cfg config.Config) (*Env, error) {
	elastic, err := elastic.New(cfg.Elastic.URL)
	if nil != err {
		return nil, err
	}
	log.Println("Elastic connected!")

	rabbitmq, err := rabbitmq.New(cfg.Rabbitmq.URL)
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
	cfg, err := config.New("config.json")
	failOnError(err, "Failed to read config.json")

	env, err := connect(cfg)
	failOnError(err, "Failed to connect to ES")

	n := negroni.Classic()

	// Routing
	mux := goji.NewMux()
	mux.HandleFunc(pat.Get("/search/:term"), env.handleSearchRequest)
	mux.HandleFunc(pat.Get("/"), env.handleHomeRequest)

	n.UseHandler(mux)

	// Middlewares
	c := cors.New(cors.Options{
		AllowedOrigins: []string{"http://0.0.0.0"},
	})
	n.Use(c)
	n.Use(gzip.Gzip(gzip.DefaultCompression))

	// Launch the Web Server
	addr := fmt.Sprintf("0.0.0.0:%s", os.Getenv("PORT"))
	srv := &http.Server{
		Handler:      n,
		Addr:         addr,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	fmt.Println("Server run on http://" + addr)
	log.Fatal(srv.ListenAndServe())
}