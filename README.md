## DaoMQ

### 介绍

一个小型、稳定、低依赖、支持并发的代码侵入式消息队列，目前支持的消息持久化方式为数据库（mysql），这是go的版本。

[DaoMQ方案设计](https://dwiki.daocloud.io/pages/viewpage.action?pageId=82438164)

### 目前支持功能

- 消息多队列支持
- 队列并发消费支持
- 消息取消支持
- 队列禁用支持
- 消息排队查询

### 用法

1. 初始化DaoMQ

   ```golang
    daomq.NewDaoMQ(SetDBDSNOption("root:dangerous@tcp(xxx:xxx)/kongtianbei"))
   ```
**SetExitCOption**: 设置信号监听通道，用于优雅退出。

   

2. 生产者发布消息

   ```golang
    p := NewProducer()
	if err := p.QueueDeclare("abc", 1); err != nil {
		t.Fatal(err)
	}
	mapA := map[string]string{
		"a": "ddddd",
		"c": "ddddd",
	}
	a, _ := json.Marshal(mapA)
	if msgId, err := p.PushMsg(string(a)); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(msgId)
	}
   ```

   

3. 生产者作废一条消息

   ```golang
	p := NewProducer()
    p.CancelMsg("msgId")
   ```

1. 生产者作废一条队列

   ```golang
    p := NewProducer()
    p.AbandonQueue("msgId")
   ```

2. 注册消费者

`basic_consume` 用于注册消费这函数
**queue**: 监听队列名。

**consumer**: 消费者函数。

**isBlocking**: 是否阻塞方式注册。

- 是：该方法会一直阻塞，当有消息被分配时会执行注册函数，执行完继续阻塞（注意不再协程执行会阻塞程序主线程）
- 否：没有消息会抛出固定异常错误ErrDaoMQNoBlocking， 否则就一直执行消费者函数

**autoAck**: 是否自动ack消息，如果填入false，也无妨需要手动保证消息能够解除`unacked`状态

```golang
c := NewConsumer()
// 
if err := c.BasicConsume("abc", a, false, true); err != nil && err != ErrDaoMQNoBlocking {
	t.Fatal(err)
}
```
