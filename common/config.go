package common

import (
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-yaml/yaml"
	log "github.com/rowdyroad/go-simple-logger"
)

func LoadConfigFromFile(config interface{}, configFile string) {
	log.Debugf("Reading configuration from '%s'", configFile)
	file, err := os.Open(configFile)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(config); err != nil {
		panic(err)
	}

	// Пробуем прочитать кастомный конфиг
	configFile = filepath.Join(
		filepath.Dir(configFile),
		strings.TrimSuffix(filepath.Base(configFile), filepath.Ext(configFile))+".custom"+filepath.Ext(configFile),
	)
	log.Debugf("Try to read custom configuration from '%s'...", configFile)
	file, err = os.Open(configFile)
	if err == nil {
		defer file.Close()
		log.Debugf("Reading custom configuration from '%s'", configFile)
		decoder = yaml.NewDecoder(file)
		if err := decoder.Decode(config); err != nil {
			panic(err)
		}
		log.Debugf("Reading custom configuration from '%s' done", configFile)
	}

	log.Debug("Config loaded successfully")
}

func LoadConfig(config interface{}, defaultFilename string) {
	var configFile string
	flag.StringVar(&configFile, "c", defaultFilename, "Config file")
	flag.StringVar(&configFile, "config", defaultFilename, "Config file")
	flag.Parse()
	LoadConfigFromFile(config, configFile)
}
