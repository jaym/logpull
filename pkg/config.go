package logpull

import (
	"crypto/tls"
)

type ServerConfig struct {
	ListenAddress string
	Path          string
	ServerCert    tls.Certificate
	ClientCaCert  []byte
}
