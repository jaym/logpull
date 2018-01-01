package logpull

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

type Server struct {
	store *Store
}

type FeedAppendRequest struct {
	FilePath string
}

type FeedReadResponse struct {
	Next  uint64
	Files []FeedFileDesc
}

type FeedFileDesc struct {
	Id       uint64
	FileName string
}

func Spawn(config ServerConfig) error {
	store, err := NewStore(config.Path)

	if err != nil {
		return err
	}

	server := &Server{
		store: store,
	}

	r := mux.NewRouter()
	r.HandleFunc("/append/{feed}", func(w http.ResponseWriter, r *http.Request) {
		server.feedAppend(w, r)
	}).Methods("POST")

	r.HandleFunc("/read/{feed}", func(w http.ResponseWriter, r *http.Request) {
		server.feedRead(w, r)
	}).Methods("GET")

	r.HandleFunc("/download/{feed}/{id}", func(w http.ResponseWriter, r *http.Request) {
		server.download(w, r)
	}).Methods("GET")

	clientCaCertPool := x509.NewCertPool()
	clientCaCertPool.AppendCertsFromPEM(config.ClientCaCert)

	tlsConfig := &tls.Config{
		ClientCAs:    clientCaCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{config.ServerCert},
	}

	httpServer := &http.Server{
		Addr:      config.ListenAddress,
		Handler:   r,
		TLSConfig: tlsConfig,
	}

	conn, err := net.Listen("tcp", config.ListenAddress)

	if err != nil {
		return nil
	}

	return httpServer.Serve(tls.NewListener(conn, tlsConfig))
}

func (s *Server) download(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	feed := vars["feed"]
	if feed == "" {
		http.Error(w, "Feed not specified", 400)
		return
	}
	idStr := vars["id"]
	if idStr == "" {
		http.Error(w, "Id not specified", 400)
		return
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Could not parse id", 400)
		return
	}

	fd, err := s.store.Get(feed, id)

	if err != nil {
		http.Error(w, "Not found", 404)
		return
	}

	f, err := os.Open(fd.FilePath)
	if err != nil {
		http.Error(w, "Not found", 404)
		return
	}
	defer f.Close()

	filename := path.Base(fd.FilePath)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))

	http.ServeContent(w, r, filename, time.Unix(0, 0), f)

}

func (s *Server) feedRead(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	feed := vars["feed"]
	if feed == "" {
		http.Error(w, "Feed not specified", 400)
		return
	}

	var since uint64
	var err error
	sinceParam := r.URL.Query().Get("since")
	if sinceParam == "" {
		since = 0
	} else {
		since, err = strconv.ParseUint(sinceParam, 10, 64)
		if err != nil {
			logrus.WithError(err).Error("Could not parse request")
			http.Error(w, "since not valid", 400)
			return
		}
	}

	logrus.WithFields(logrus.Fields{
		"feed":  feed,
		"since": since,
	}).Info("Reading feed")

	files, next, err := s.store.ReadFeed(feed, since)

	if err != nil {
		logrus.WithError(err).Error("Could not read feed")
		http.Error(w, "Could not read feed", 500)
		return
	}

	respFiles := []FeedFileDesc{}

	for _, v := range files {
		respFiles = append(respFiles, FeedFileDesc{
			Id:       v.Id,
			FileName: path.Base(v.FilePath),
		})
	}

	resp := &FeedReadResponse{
		Next:  next,
		Files: respFiles,
	}

	json.NewEncoder(w).Encode(resp)
}

func (s *Server) feedAppend(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	feed := vars["feed"]
	if feed == "" {
		http.Error(w, "Feed not specified", 400)
		return
	}

	decoder := json.NewDecoder(r.Body)
	var body FeedAppendRequest
	err := decoder.Decode(&body)

	if err != nil {
		logrus.WithError(err).Error("Could not parse request")
		http.Error(w, "Could not parse request", 400)
		return
	}

	logrus.WithFields(logrus.Fields{
		"feed": feed,
		"file": body.FilePath,
	}).Info("Adding file")

	err = s.store.AppendFileToFeed(feed, body.FilePath)

	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"feed": feed,
		}).Error("Could not append file to feed")
		// This could fail for non 500 reasons, but whatever
		http.Error(w, "Could not append file", 500)
		return
	}
}
