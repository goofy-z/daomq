package daomq

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"k8s.io/klog/v2"
)

const (
	MSGStatusReady  = "ready"
	MSGStatusUnACK  = "unacked"
	MSGStatusACK    = "ack"
	MSGStatusCancel = "cancel"

	// 队列开放状态
	QueueStatusOpen = "open"

	// 队列废弃状态
	QueueStatusAbandon = "abandon"
)

var (
	ErrDaoMQNotInit           = fmt.Errorf("DaoMQ not init")
	ErrDaoMQQueueNotExist     = fmt.Errorf("DaoMQ queue not exist")
	ErrDaoMQNoMsgPop          = fmt.Errorf("DaoMQ queue pop msg failed")
	ErrDaoMQMsgNotFound       = fmt.Errorf("DaoMQ msg not found")
	ErrDaoMQNoBlocking        = fmt.Errorf("DaoMQ queue no msg and not blocking")
	ErrDaoMQNotResetMsg       = fmt.Errorf("DaoMQ not reset the failed msg")
	ErrDaoMQQueueAbandon      = fmt.Errorf("DaoMQ queue has been abandoned")
	ErrDaoMQBrokerExit        = fmt.Errorf("DaoMQ Broker has exit")
	ErrDaoMQMsgStatusAbnormal = fmt.Errorf("DaoMQ msg is Abnormal")
	ErrDaoMQDBFailed          = fmt.Errorf("DaoMQ db operate is failed ")
)

type LoopStatus struct {
	ch chan *QueueMSGRecord
}

type Broker struct {
	*DB
	c             *Config
	IsExit        bool
	isExitC       chan struct{}
	Lock          sync.RWMutex
	cacheLock     sync.RWMutex
	ConsumerList  []string
	loopStatusMap map[string]*LoopStatus
	cacheTaskMap  map[string]time.Time
}

func NewBroker(c *Config, db *DB) *Broker {
	var lst []string
	broker := &Broker{
		DB:            db,
		c:             c,
		isExitC:       c.isExitC,
		ConsumerList:  lst,
		Lock:          sync.RWMutex{},
		cacheLock:     sync.RWMutex{},
		cacheTaskMap:  make(map[string]time.Time),
		loopStatusMap: make(map[string]*LoopStatus),
	}
	broker.gracefulDown()
	return broker
}

func (b *Broker) gracefulDown() {
	go func() {
		<-b.isExitC
		// 开始优雅退出
		klog.Warning("DaoMQ graceful down")
		b.IsExit = true
		if len(b.ConsumerList) != 0 {
			newList := []string{}
			for _, c := range b.ConsumerList {
				newList = append(newList, fmt.Sprintf("'%s'", c))
			}
			stmt := fmt.Sprintf(ResetMessageStmt, b.c.MsgQueueTable, MSGStatusReady, MSGStatusUnACK, strings.Join(newList, ","))
			_, err := b.DB.Exec(stmt)
			if err != nil {
				klog.Error(err)
			}
		}
		defer b.DB.Close()
		klog.Warning("DaoMQ graceful donw finished")
	}()
}

// 向Broker注册consumer
func (b *Broker) RegisterConsumer(consumerId string) {
	b.Lock.Lock()
	defer b.Lock.Unlock()
	b.ConsumerList = append(b.ConsumerList, consumerId)
}

// pop 消息
func (b *Broker) Pop(queue string, consumerId string, isBlocking bool) (task *QueueMSGRecord, err error) {
	// 这里的轮训是为了任务出列后，执行绑定动作失败而再一次执行任务出列函数。
	// 这个失败原因是：另一个消费者协程or进程绑定任务成功，则当前消费者自动绑定失败
	for {
		// 非阻塞情况下将会主动轮训表数据
		if isBlocking {
			// 检查是否已经启动了loop
			var loop *LoopStatus
			var ok bool
			loop, ok = b.loopStatusMap[queue]
			if !ok {
				b.Lock.Lock()
				// 获取锁之后再检查一次是否已经启动了loop, 第一个获取锁的map里肯定不存在该queue
				loop, ok = b.loopStatusMap[queue]
				if !ok {
					loop = &LoopStatus{}
					loop.ch = make(chan *QueueMSGRecord, 1)
					b.loopStatusMap[queue] = loop
					go b.TheOneLoop(queue)
				}
				b.Lock.Unlock()

			}
			task, ok = <-loop.ch
			// 代表loop已经退出
			if !ok {
				return nil, ErrDaoMQBrokerExit
			}
		} else {
			task, err = b.findTask(queue)
			if task == nil && err == nil {
				return nil, ErrDaoMQNoBlocking
			}
			if err != nil {
				return task, err
			}
		}
		// 检查是否绑定成功
		if isSuccess, err := b.bindTask(task, consumerId); isSuccess {
			return task, err
		}
		// 绑定失败则重新执行 任务出列
	}

}

func (b *Broker) bindTask(task *QueueMSGRecord, consumerId string) (bool, error) {
	// 4 绑定任务

	// 先检查是否已经收到退出通知
	if b.IsExit {
		klog.Warning("DaoMQ Broker status is change, so exit")
		return false, ErrDaoMQBrokerExit
	}
	uTime := time.Now().Format("2006-01-02 15:04:05")
	stmt := fmt.Sprintf(BindMessageByIdStmt, b.c.MsgQueueTable, MSGStatusUnACK, uTime, consumerId, task.Id)
	res, err := b.DB.Exec(stmt)
	if err != nil {
		return false, err
	}

	// 5 确认绑定状态
	num, err := res.RowsAffected()
	if err == nil && num != 0 {
		b.DeleteCache(task)
		return true, nil
	}
	return false, err
}

// 任务出列，从队列中取出待分配任务
func (b *Broker) findTask(queue string) (*QueueMSGRecord, error) {
	if b.IsExit {
		return nil, ErrDaoMQBrokerExit
	}
	q := QueueRecord{}
	UnACKmsgs := []*QueueMSGRecord{}

	// 1 检查队列状态
	stmt := fmt.Sprintf(SelectQueueByNameStmt, b.c.QueueTable, queue)
	if err := b.DB.QueryRow(stmt).Scan(&q.Queue, &q.Status, &q.Concurrent); err != nil {
		return nil, ErrDaoMQQueueNotExist
	}
	if q.Status != QueueStatusOpen {
		return nil, ErrDaoMQQueueAbandon
	}

	// 2 找出当前队列正在消费个数
	stmt = fmt.Sprintf(SelectMessageByStatusStmt, b.c.MsgQueueTable, queue, MSGStatusUnACK)
	rows, err := b.DB.Query(stmt)
	if err != nil {
		return nil, ErrDaoMQDBFailed
	}
	for rows.Next() {
		msg := &QueueMSGRecord{}
		if err = rows.Scan(&msg.Id, &msg.Data); err != nil {
			return nil, ErrDaoMQDBFailed
		}
		UnACKmsgs = append(UnACKmsgs, msg)
	}
	rows.Close()
	// 当前处理的任务个数大于或等于并发数则退出循环
	restTask := q.Concurrent - len(UnACKmsgs)
	if restTask <= 0 {
		// 达到最大处理任务数清理一次cacheTask
		b.cacheLock.Lock()
		b.cacheTaskMap = make(map[string]time.Time)
		b.cacheLock.Unlock()
		return nil, nil
	}

	// 3 找出待绑定数量为 restTask 条记录
	stmt = fmt.Sprintf(SelectOneMessageByStatusStmt, b.c.MsgQueueTable, queue, MSGStatusReady, restTask)
	rows, err = b.DB.Query(stmt)
	if err != nil {
		return nil, ErrDaoMQDBFailed
	}
	for rows.Next() {
		targetTask := &QueueMSGRecord{}
		if err := rows.Scan(&targetTask.Id, &targetTask.Data); err != nil {
			return nil, ErrDaoMQDBFailed
		}
		// 判断task是否在cache中, 不在cache中,则先假定该任务，然后返回
		if !b.IsAssumed(targetTask) {
			rows.Close()
			b.Assume(targetTask)
			return targetTask, nil
		}
	}
	rows.Close()
	// 没有找到代调度任务则返回空
	return nil, nil
}

// 当前broker内队列的唯一loop来检查是否有待消费任务
func (b *Broker) TheOneLoop(queue string) error {
	loop := b.loopStatusMap[queue]
	defer func() {
		// 关闭通道,清理map
		close(loop.ch)
		delete(b.loopStatusMap, queue)
	}()
	for {
		task, err := b.findTask(queue)
		if err != nil {
			klog.Error(err)
			return err
		}
		if task == nil {
			// 任务出列失败则等待0.5s 在查询队列表
			time.Sleep(time.Millisecond * 500)
			continue
		}
		select {
		// 监听主协程退出
		case <-b.isExitC:
			klog.Warning("DaoMQ end exit loop: ", queue)
			return ErrDaoMQBrokerExit
		// 发送task到通道
		case loop.ch <- task:
			klog.Info("DaoMQ Assume task success ", task.Id)
		}

	}
}

// 假定task已经被绑定了
func (b *Broker) Assume(task *QueueMSGRecord) {
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()
	b.cacheTaskMap[task.Id] = time.Now()
}

// 判断task是否已经被绑定
func (b *Broker) IsAssumed(task *QueueMSGRecord) bool {
	b.cacheLock.RLock()
	defer b.cacheLock.RUnlock()
	_, ok := b.cacheTaskMap[task.Id]
	return ok
}

// 从cache中删除这个task， 如果多进程消费同一个队列可能无法删除这个缓存，最终这个key会一直存在
func (b *Broker) DeleteCache(task *QueueMSGRecord) {
	b.cacheLock.Lock()
	defer b.cacheLock.Unlock()
	delete(b.cacheTaskMap, task.Id)
	klog.Info("DaoMQ Bind task success: ", task.Id)
}

// 向指定队列推送消息
func (b *Broker) Push(queue string, data interface{}) (string, error) {
	cTime := time.Now().Format("2006-01-02 15:04:05")
	id := uuid.New().String()
	stmt := fmt.Sprintf(CreateMessageStmt, b.c.MsgQueueTable, id, cTime, cTime, queue, data)
	_, err := b.DB.Exec(stmt)
	return id, err
}

// ack消息
func (b *Broker) AckMsg(msgId string, consumerId string) error {
	uTime := time.Now().Format("2006-01-02 15:04:05")
	if msgId != "" {
		stmt := fmt.Sprintf(UpdateMessageStatusStmt, b.c.MsgQueueTable, MSGStatusACK, uTime, MSGStatusUnACK, msgId)
		_, err := b.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// 根据id获取消息
func (b *Broker) GetMsg(msgId string) (status string, data string, err error) {
	if msgId != "" {
		stmt := fmt.Sprintf(SelectMessageByIdStmt, b.c.MsgQueueTable, msgId)
		q := &QueueMSGRecord{}
		if err := b.DB.QueryRow(stmt).Scan(&q.Status, &q.Data); err != nil {
			return "", "", ErrDaoMQMsgNotFound
		}
		return q.Status, q.Data, nil
	}
	return
}

// ready消息
func (b *Broker) ReadyMsg(msgId string) error {
	uTime := time.Now().Format("2006-01-02 15:04:05")
	if msgId != "" {
		stmt := fmt.Sprintf(UpdateMessageStatusStmt, b.c.MsgQueueTable, MSGStatusACK, uTime, MSGStatusUnACK, msgId)
		_, err := b.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// cancel消息
func (b *Broker) CancelMsg(msgId string) error {
	uTime := time.Now().Format("2006-01-02 15:04:05")
	if msgId != "" {
		stmt := fmt.Sprintf(UpdateMessageStatusByIdStmt, b.c.MsgQueueTable, MSGStatusCancel, uTime, msgId)
		_, err := b.DB.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// 检查队列状态
func (b *Broker) CheckQueue(queue string) (queue_name string, status string, concurrent int, err error) {
	stmt := fmt.Sprintf(SelectQueueByNameStmt, b.c.MsgQueueTable, queue)
	q := &QueueRecord{}
	if err = b.DB.QueryRow(stmt).Scan(&q.Queue, &q.Status); err != nil {
		return "", "", 0, ErrDaoMQQueueNotExist
	}
	return q.Queue, q.Status, q.Concurrent, nil
}

// 创建队列
func (b *Broker) CreateQueue(queue string, concurrent int) {
	cTime := time.Now().Format("2006-01-02 15:04:05")
	stmt := fmt.Sprintf(CreateQueueRecordStmt, b.c.QueueTable, cTime, cTime, QueueStatusOpen, queue, concurrent)
	b.DB.Exec(stmt)
}

// 废弃队列
func (b *Broker) AbandonQueue(queue string) error {
	cTime := time.Now().Format("2006-01-02 15:04:05")
	stmt := fmt.Sprintf(UpdateQueueStatusByIdStmt, b.c.QueueTable, QueueStatusAbandon, cTime, queue)
	if _, err := b.DB.Exec(stmt); err != nil {
		return err
	}
	return nil
}

// 查询队列
func (b *Broker) GetOpenQueue() ([]*QueueRecord, error) {
	queues := []*QueueRecord{}
	stmt := fmt.Sprintf(SelectQueueByStatusStmt, b.c.QueueTable, QueueStatusOpen)
	rows, err := b.DB.Query(stmt)
	if err != nil {
		return queues, err
	}
	for rows.Next() {
		queue := &QueueRecord{}
		rows.Scan(&queue.Queue, &queue.Status, &queue.Concurrent)
		queues = append(queues, queue)
	}
	return queues, nil
}
