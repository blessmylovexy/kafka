package kafka

import (
	"strings"
	"time"

	"github.com/blessmylovexy/log"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

/*StartLowVersionKafkaConsumer 启动低版本kafka消费组*/
func StartLowVersionKafkaConsumer(zookeeper, topics, groupID string, receiver chan *sarama.ConsumerMessage) {
	var zookeeperNodes []string

	consumerGroupConfig := consumergroup.NewConfig()
	consumerGroupConfig.Offsets.Initial = sarama.OffsetNewest

	consumerGroupConfig.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, consumerGroupConfig.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeper)
	kafkaTopics := strings.Split(topics, ",")

	consumer, err := consumergroup.JoinConsumerGroup(groupID, kafkaTopics, zookeeperNodes, consumerGroupConfig)
	if err != nil {
		log.Fatal("创建Kafka消费者失败:%s", err)
	}

	log.Infof("Kafka消费者启动成功,groupID:%s,topics:%s", groupID, topics)

	go func() {
		for err := range consumer.Errors() {
			log.Errorf("消费Kafka数据失败:%s", err)
		}
	}()

	for message := range consumer.Messages() {
		receiver <- message
		consumer.CommitUpto(message)
	}
}
