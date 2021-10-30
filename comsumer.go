package daomq

import (
	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

type Consumer struct {
	isBlocking bool
	ConsumerId string
	Queue      string
	F          ConsumerFunc
	Extra      []interface{}
}

type ConsumerFunc func(task *QueueMSGRecord)

// 初始化生产者
func NewConsumer() *Consumer {
	return &Consumer{
		ConsumerId: uuid.New().String(),
	}
}

// 注册并启动消费者，注意这是运行在主线程中，会使其阻塞，如果需启动多个消费者则考虑放到协程中
func (c *Consumer) BasicConsume(queue string, f ConsumerFunc, isBlocking bool, autoAck bool) error {
	// 设置消费队列
	c.Queue = queue
	c.isBlocking = isBlocking
	c.F = f
	for {
		task, err := BrokerManager.Pop(c.Queue, c.ConsumerId, c.isBlocking)
		// pop消息出错直接返回
		if err != nil {
			klog.Error(err)
			return err
		}

		err = c.consumeOne(task)
		if err != nil {
			// 恢复这条记录
			BrokerManager.ReadyMsg(task.Id)
		} else {
			// 是否自动ACK
			if autoAck {
				if err = BrokerManager.AckMsg(task.Id, c.ConsumerId); err != nil {
					klog.Error(err)
					return err
				}
			}
		}
	}
}

func (c *Consumer) consumeOne(task *QueueMSGRecord) error {
	var err error
	// 捕捉注册函数的异常
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			klog.Errorf("DaoMQ Register Func do error, taskId %s, %s", task.Id, err)
		}
	}()

	// 执行消费者注册函数
	c.F(task)
	return err
}
