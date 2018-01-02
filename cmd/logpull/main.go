package main

import (
	"github.com/BurntSushi/toml"
	logpull "github.com/jaym/logpull/pkg"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "logpull",
	Short: "logpull is a HTTP file server",
}

var srvCmd = &cobra.Command{
	Use:   "serve configFile",
	Short: "Start the file server",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var conf logpull.ConfigFile
		_, err := toml.DecodeFile(args[0], &conf)
		if err != nil {
			logrus.WithError(err).Fatal("Could not load config")
		}
		serverConf, err := conf.ToServerConfig()
		if err != nil {
			logrus.WithError(err).Fatal("Could not load config")
		}

		logpull.Spawn(serverConf)
	},
}

func init() {
	rootCmd.AddCommand(srvCmd)
}

func main() {
	rootCmd.Execute()
}
