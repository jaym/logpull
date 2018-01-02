package logpull

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
)

type ServerConfig struct {
	ListenAddress string
	Path          string
	ServerCert    tls.Certificate
	ClientCaCert  []byte
}

type ConfigFile struct {
	CertPath string            `toml:"cert"`
	KeyPath  string            `toml:"key"`
	Server   *ConfigFileServer `toml:"server"`
}

type ConfigFileServer struct {
	ListenAddress    string `toml:"listen_address"`
	Path             string `toml:"path"`
	ClientCaCertPath string `toml:"client_ca_cert"`
}

func (c *ConfigFile) ToServerConfig() (ServerConfig, error) {
	if c.Server == nil {
		return ServerConfig{}, fmt.Errorf("Server configuration not provided")
	}

	if c.Server.ListenAddress == "" {
		return ServerConfig{}, fmt.Errorf("server.listen_address not provided")
	}

	if c.Server.Path == "" {
		return ServerConfig{}, fmt.Errorf("server.path not provided")
	}

	if c.Server.ClientCaCertPath == "" {
		return ServerConfig{}, fmt.Errorf("server.path not provided")
	}

	cert, err := tls.LoadX509KeyPair(c.CertPath, c.KeyPath)
	if err != nil {
		return ServerConfig{}, err
	}

	clientCaCert, err := ioutil.ReadFile(c.Server.ClientCaCertPath)
	if err != nil {
		return ServerConfig{}, err
	}

	return ServerConfig{
		ListenAddress: c.Server.ListenAddress,
		Path:          c.Server.Path,
		ClientCaCert:  clientCaCert,
		ServerCert:    cert,
	}, nil
}
