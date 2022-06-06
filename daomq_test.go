package daomq

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	NewDaoMQ(SetDBDSNOption("root:dangerous@tcp(10.21.5.32:34115)/kongtianbei"))
	ret := m.Run()
	os.Exit(ret)
}

func TestPush(t *testing.T) {
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
}

func TestQueueStatus(t *testing.T) {
	queues, err := BrokerManager.GetOpenQueue()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("queues: %v %d\n", queues, len(queues))
}

func TestPop(t *testing.T) {
	exitFlag := false
	a := func(task TaskHandler) {
		exitFlag = true
		fmt.Println(task.GetTask())
	}
	c := NewConsumer()
	for {
		task, err := c.BasicConsume("abc", a, true, false)
		if err != nil && err != ErrDaoMQNoBlocking {
			t.Fatal(err)
		}
		if err := BrokerManager.AckMsg(task.Id, c.ConsumerId); err != nil {
			t.Fatal(err)
		}
		if exitFlag {
			break
		}
	}
}
