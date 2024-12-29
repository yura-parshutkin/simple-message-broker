package server

import (
	"brocker/pkg/mule"
	"context"
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
)

type HttpServer struct {
	srv *http.Server
}

func NewHttpServer(
	addr string,
	config mule.Config,
) mule.Server {
	broker := mule.NewBroker(config)
	broker.Run()

	ro := mux.NewRouter()
	ro.Methods("POST").Path("/v1/queues/{queue}/messages").Handler(NewAddMessageHandler(broker))
	ro.Methods("POST").Path("/v1/queues/{queue}/subscriptions").Handler(NewAddSubscriptionHandler(broker))
	srv := &http.Server{
		Addr:    addr,
		Handler: ro,
	}
	return &HttpServer{
		srv: srv,
	}
}

func (a *HttpServer) Run() error {
	if err := a.srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("queue stopped listening: %qw", err)
	}
	return nil
}

func (a *HttpServer) Stop(ctx context.Context) error {
	return a.srv.Shutdown(ctx)
}

type AddMessageHandler struct {
	broker *mule.Broker
}

func NewAddMessageHandler(broker *mule.Broker) http.Handler {
	return &AddMessageHandler{
		broker: broker,
	}
}

func (p *AddMessageHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	query := vars["queue"]
	body, errBo := io.ReadAll(r.Body)
	if errBo != nil {
		http.Error(w, errBo.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	err := p.broker.AddMessage(ctx, query, body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type AddSubscriptionHandler struct {
	broker *mule.Broker
}

func NewAddSubscriptionHandler(broker *mule.Broker) http.Handler {
	return &AddSubscriptionHandler{broker: broker}
}

func (p *AddSubscriptionHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")

	vars := mux.Vars(r)
	query := vars["queue"]
	ctx := r.Context()
	subscribe, err := p.broker.Subscribe(ctx, query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for m := range subscribe {
		_, _ = fmt.Fprintf(w, "%s\n\n", string(m))
		flusher.Flush()
	}
}
