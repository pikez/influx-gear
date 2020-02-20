package main

import (
	"flag"
	"gear/config"
	"gear/service"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {

	configFile := flag.String("config", "./influx_gear_test.conf", "give a file path for config file")
	logLevel := flag.String("loglevel", "info", "give log level")

	flag.Parse()
	logrusLevel, err := log.ParseLevel(*logLevel)
	if err != nil {
		logrusLevel = log.WarnLevel
	}
	log.SetLevel(logrusLevel)
	cfg := config.Config(*configFile)
	cfg.WithDefaults()

	gear := service.NewGearService(*cfg)
	gear.Run()
}

func init() {
	log.SetReportCaller(true)
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
}
