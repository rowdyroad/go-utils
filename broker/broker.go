package broker

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"regexp"
	"sync"
	"time"

	nsq "github.com/nsqio/go-nsq"
	log "github.com/rowdyroad/go-simple-logger"
)

var invalidChannelChars = regexp.MustCompile(`[^\.a-zA-Z0-9_-]`)

type RoutesConfig struct {
	Topics []string `yaml:"topics" json:"topics"`
	Strict bool     `yaml:"strict" json:"strict"`
}

type SubscriptionConfig struct {
	Topic   string `yaml:"topic" json:"topic"`
	Channel string `yaml:"channel" json:"channel"`
}

type Config struct {
	Address       string                          `yaml:"address" json:"address"`
	LookupAddress string                          `yaml:"lookupAddress" json:"lookup_address"`
	Routes        map[string]RoutesConfig         `yaml:"routes" json:"routes"`
	Subscriptions map[string][]SubscriptionConfig `yaml:"subscriptions" json:"subscriptions"`
}

type Broker struct {
	sync.Mutex
	config     Config
	encoder    func(data []byte, v interface{}) error
	decoder    func(v interface{}) ([]byte, error)
	consumers  map[string]*[]*nsq.Consumer
	producer   *nsq.Producer
	statsGuard *sync.Mutex
	stats      *map[string]uint64
}

//NewBroker creates Broker instance. Wait for config and encode/decode functions.
func NewBroker(config Config, encoder func(data []byte, v interface{}) error, decoder func(v interface{}) ([]byte, error)) *Broker {
	if config.Routes == nil {
		config.Routes = map[string]RoutesConfig{}
	}

	if config.Subscriptions == nil {
		config.Subscriptions = map[string][]SubscriptionConfig{}
	}

	stats := &map[string]uint64{}
	statsGuard := &sync.Mutex{}

	go func() {
		for {
			func() {
				statsGuard.Lock()
				defer statsGuard.Unlock()
				for what, count := range *stats {
					log.Infof("Stat for: %s - %d", what, count)
					delete(*stats, what)
				}
			}()
			time.Sleep(10 * time.Second)
		}
	}()

	return &Broker{
		sync.Mutex{},
		config,
		encoder,
		decoder,
		map[string]*[]*nsq.Consumer{},
		nil,
		statsGuard,
		stats,
	}
}

func (b *Broker) HasRoute(message string) bool {
	b.Lock()
	defer b.Unlock()
	_, has := b.config.Routes[message]
	return has
}

func (b *Broker) AddSubscription(subscription, topic, channel string) {
	b.Lock()
	defer b.Unlock()

	_, has := b.config.Subscriptions[subscription]
	if !has {
		b.config.Subscriptions[subscription] = []SubscriptionConfig{}
	}

	b.config.Subscriptions[subscription] = append(b.config.Subscriptions[subscription], SubscriptionConfig{
		Topic:   topic,
		Channel: channel,
	})
}
func (b *Broker) RemoveSubscription(subscription string) {
	b.Lock()
	defer b.Unlock()
	delete(b.config.Subscriptions, subscription)
	if consumers, has := b.consumers[subscription]; has {
		for _, consumer := range *consumers {
			consumer.Stop()
		}
		delete(b.consumers, subscription)
	}
}

func (b *Broker) AddRoute(route string, strict bool, topics ...string) {
	b.Lock()
	defer b.Unlock()
	b.config.Routes[route] = RoutesConfig{
		Topics: topics,
		Strict: strict,
	}
}
func (b *Broker) RemoveRoute(route string) {
	b.Lock()
	defer b.Unlock()
	delete(b.config.Routes, route)
}

//Subscribe for message from broker. Callback must be func(item <type of message>) error.
func (b *Broker) Subscribe(message string, callback interface{}) {
	log.Debug("Subscribe for message:", message)
	config := nsq.NewConfig()
	config.MaxAttempts = 0
	
	t := reflect.TypeOf(callback)
	if t.NumIn() != 1 {
		log.Crit("Incorrect callback signature")
	}

	subscribes := func() []SubscriptionConfig {
		b.Lock()
		defer b.Unlock()
		subscribes, has := b.config.Subscriptions[message]
		if !has {
			log.Critf("Subscriptions for message '%s' not found", message)
		}
		return subscribes
	}()

	for _, subscribe := range subscribes {
		if subscribe.Channel == "" {
			subscribe.Channel = string(invalidChannelChars.ReplaceAll([]byte(path.Base(os.Args[0])), []byte(".")))
			log.Debug("Channel for", subscribe.Topic, "is not defined. Auto channel name is", subscribe.Channel)
		}
		//todo: optimize consumers create and marshaling for equals callback
		if c, err := nsq.NewConsumer(subscribe.Topic, subscribe.Channel, config); err == nil {
			c.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
				b.statsGuard.Lock()
				(*b.stats)[fmt.Sprintf("%s / %s", message, subscribe.Topic)]++
				b.statsGuard.Unlock()
				elem := reflect.New(t.In(0))
				if err := b.encoder(msg.Body, elem.Interface()); err == nil {
					ret := reflect.ValueOf(callback).Call([]reflect.Value{elem.Elem()})
					if len(ret) > 0 && !ret[0].IsNil() {
						return ret[0].Interface().(error)
					}
				} else {
					log.Error("Error encoding:", err, string(msg.Body))
				}
				return nil
			}))

			var err error
			if b.config.LookupAddress != "" {
				log.Debug("Connecting to nsqd lookup", b.config.LookupAddress)
				err = c.ConnectToNSQLookupd(b.config.LookupAddress)
			} else {
				log.Debug("Connecting to nsqd", b.config.Address)
				err = c.ConnectToNSQD(b.config.Address)
			}
			if err != nil {
				log.Crit("Could not connect")
			}

			b.Lock()
			consumers, has := b.consumers[message]
			if !has {
				consumers = &[]*nsq.Consumer{}
				b.consumers[message] = consumers
			}
			*consumers = append(*consumers, c)
			b.Unlock()
		} else {
			log.Crit("New consumer error:", err, subscribe)
		}
	}
}

//Publish message to broker with custom config.
func (b *Broker) Publish(message string, data interface{}) error {
	return b.PublishWithConfig(message, data, nsq.NewConfig())
}

//PublishWithConfig message to broker with custom config.
func (b *Broker) PublishWithConfig(message string, data interface{}, config *nsq.Config) error {
	log.Debug("Publish message:", message)
	routes, has := b.config.Routes[message]
	if !has {
		log.Critf("Route for message '%s' not found", message)
	}

	if b.producer == nil {
		w, err := nsq.NewProducer(b.config.Address, config)
		if err != nil {
			return err
		}
		b.producer = w
	}

	bb, err := b.decoder(data)
	if err != nil {
		return err
	}

	rts := map[string][][]byte{}

	for _, route := range routes.Topics {
		if _, has := rts[route]; !has {
			rts[route] = [][]byte{}
		}
		rts[route] = append(rts[route], bb)
	}

	for route, data := range rts {
		if err := b.producer.MultiPublish(route, data); err != nil {
			log.Error("Publish error:", err)
			if routes.Strict {
				return err
			}
		}
	}

	return nil
}

func (b *Broker) Close() {
	b.Lock()
	defer b.Unlock()
	log.Debug("Stopping consumers")
	for _, consumers := range b.consumers {
		for _, consumer := range *consumers {
			consumer.Stop()
		}
	}
}
