package daomq

var BrokerManager *Broker

func NewDaoMQ(opts ...Option) {
	c := NewConfig()
	for _, opt := range opts {
		opt(c)
	}

	// 初始化db
	db, err := NewDB(c)
	if err != nil {
		panic(err)
	}

	// 初始化broker
	BrokerManager = NewBroker(c, db)
}
