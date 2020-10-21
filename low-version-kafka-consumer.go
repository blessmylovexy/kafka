package kafka

import (
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/blessmylovexy/log"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

/*StartLowVersionKafkaConsumer 启动低版本kafka消费组*/
func StartLowVersionKafkaConsumer(zookeeper, topics, groupID string, interval int64, receiver chan *sarama.ConsumerMessage) {
	var zookeeperNodes []string

	config := consumergroup.NewConfig()
	config.Offsets.ProcessingTimeout = 10 * time.Second
	config.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = time.Duration(interval) * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeper)
	kafkaTopics := strings.Split(topics, ",")

	consumer, err := consumergroup.JoinConsumerGroup(groupID, kafkaTopics, zookeeperNodes, config)
	if err != nil {
		log.Fatalf("创建Kafka消费者失败:%v", err)
	}

	log.Infof("Kafka消费者启动成功,groupID:%s,topics:%s", groupID, topics)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			log.Errorf("Error closing the consumer:%v", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Errorf("消费Kafka数据失败:%s", err)
		}
	}()

	for message := range consumer.Messages() {
		receiver <- message
	}
}
