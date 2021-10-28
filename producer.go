package daomq

type Producer struct {
	Queue string
}

// 初始化生产者
func NewProducer() *Producer {
	return &Producer{}
}

// 确认队列
func (p *Producer) QueueDeclare(queue string, concurrent int) error {
	// 检查DaoMQ是否初始化
	if BrokerManager == nil {
		return ErrDaoMQNotInit
	}

	// 创建队列，已存在该队列则不处理
	BrokerManager.CreateQueue(queue, concurrent)

	p.Queue = queue
	return nil
}

// 队列推送消息
func (p *Producer) PushMsg(data string) (string, error) {
	return BrokerManager.Push(p.Queue, data)
}

// 撤销消息
func (p *Producer) CancelMsg(msgId string) error {
	return BrokerManager.CancelMsg(msgId)
}

// 废弃队列
func (p *Producer) Ab(queue string) error {
	return BrokerManager.AbandonQueue(queue)
}
