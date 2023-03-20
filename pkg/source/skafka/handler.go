package skafka

import (
	"github.com/Shopify/sarama"
	"sync"
)

type ConsumerHandler struct {
	lock            *sync.RWMutex
	ReadBufferChan  chan *sessMsg
	ready           chan bool
	highWaterOffset map[int32]int64
	currentOffset   map[int32]int64
}

type sessMsg struct {
	sess sarama.ConsumerGroupSession
	msg  *sarama.ConsumerMessage
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	//claim.HighWaterMarkOffset()
	for {
		select {
		case message := <-claim.Messages():
			if message != nil {
				consumer.ReadBufferChan <- &sessMsg{
					sess: session,
					msg:  message,
				}
				consumer.lock.Lock()
				consumer.currentOffset[message.Partition] = message.Offset
				consumer.highWaterOffset[claim.Partition()] = claim.HighWaterMarkOffset()
				consumer.lock.Unlock()
			}
		case <-session.Context().Done():
			return nil
		}

	}
}
