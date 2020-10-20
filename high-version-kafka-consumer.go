package kafka

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/blessmylovexy/log"
)

/*StartHighVersionKafkaConsumer 启动高版本kafka消费组*/
func StartHighVersionKafkaConsumer(brokers, version, topics, groupID string, receiver chan *sarama.ConsumerMessage) {

	kafkaVersion, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("解析Kafka版本失败: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = kafkaVersion
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// 创建一个新的消费者组
	consumer := Consumer{
		ready: make(chan bool),
		msg:   receiver,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), groupID, config)
	if err != nil {
		log.Panicf("创建Kafka客户端失败: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// 需要在循环内调用Consume, 当server端发生rebalance时,将需要重新创建消费者。
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("创建Kafka消费者失败: %v", err)
			}
			// 关闭ctx时停止消费
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // 等待消费者启动

	log.Infof("Kafka消费者启动成功,groupID:%s,topics:%s", groupID, topics)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Info("terminating: context cancelled")
	case <-sigterm:
		log.Info("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("关闭Kafka客户端失败: %v", err)
	}
}

/*Consumer 消费者*/
type Consumer struct {
	ready chan bool
	msg   chan *sarama.ConsumerMessage
}

/*Setup 执行初始化*/
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

/*Cleanup 执行清理*/
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

/*ConsumeClaim 消费数据*/
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		consumer.msg <- message
		session.MarkMessage(message, "")
	}
	return nil
}
