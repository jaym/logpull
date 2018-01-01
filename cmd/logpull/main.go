package main

import logpull "github.com/jaym/logpull/pkg"

func main() {
	logpull.Spawn(logpull.ServerConfig{
		ListenAddress: "localhost:10000",
		Path:          "./data",
	})
}
