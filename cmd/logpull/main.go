package main

import (
	"crypto/tls"
	"io/ioutil"

	logpull "github.com/jaym/logpull/pkg"
	"github.com/sirupsen/logrus"
)

func main() {
	serverCert, err := tls.LoadX509KeyPair("./example/testserver.crt", "./example/testserver.key")
	if err != nil {
		logrus.WithError(err).Fatalf("Could not load cert")
	}

	clientCaCert, err := ioutil.ReadFile("./example/thechamberofunderstanding.com.crt")

	logpull.Spawn(logpull.ServerConfig{
		ListenAddress: "localhost:10000",
		Path:          "./data",
		ServerCert:    serverCert,
		ClientCaCert:  clientCaCert,
	})
}
