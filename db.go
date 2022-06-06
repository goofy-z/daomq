package daomq

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

const (
	CreateQueueTableStmt = `
		CREATE TABLE IF NOT EXISTS %s(
			id INT  PRIMARY KEY AUTO_INCREMENT,
			created_at TIMESTAMP NULL,
			updated_at TIMESTAMP  NULL,
			status varchar(32) DEFAULT "ready",
			queue varchar(64) NOT NULL,
			concurrent int(11) DEFAULT 1,
			UNIQUE KEY queue (queue)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	CreateMessageTableStmt = `
		CREATE TABLE IF NOT EXISTS %s(
			id varchar(36)  PRIMARY KEY,
			created_at TIMESTAMP NULL,
			updated_at TIMESTAMP  NULL,
			status varchar(32) DEFAULT "ready",
			queue varchar(64) NOT NULL,
			consumer_id varchar(36) DEFAULT NULL,
			data text,
			index ix_queue_status (queue, status)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	ResetMessageStmt = `
		UPDATE %s SET status='%s', consumer_id=null WHERE status='%s' AND consumer_id IN (%s)
	`
	CreateMessageStmt = `
		INSERT INTO %s (id, created_at, updated_at, queue, data) VALUES ('%s', '%s', '%s', '%s', '%s') 
	`
	UpdateMessageStatusStmt = `
		UPDATE %s SET status='%s', updated_at='%s' WHERE status='%s' AND id='%s'
	`
	UpdateMessageStatusByIdStmt = `
		UPDATE %s SET status='%s', updated_at='%s' WHERE id='%s'
	`
	CreateQueueRecordStmt = `
		INSERT INTO %s (created_at, updated_at, status, queue, concurrent) VALUES ('%s', '%s', '%s', '%s', %d) 
	`
	SelectMessageByStatusStmt = `
		SELECT id, data from %s where queue='%s' and status='%s' order by created_at
	`
	SelectOneMessageByStatusStmt = `
		SELECT id, data from %s where queue='%s' and status='%s' order by created_at limit %d
	`
	SelectMessageByIdStmt = `
		SELECT status, data from %s WHERE id='%s'
	`
	BindMessageByIdStmt = `
		UPDATE %s SET status='%s', updated_at='%s', consumer_id='%s' WHERE id='%s' and (consumer_id is null or consumer_id = "") and status='ready'
	`
	UpdateQueueStatusByIdStmt = `
		UPDATE %s SET status='%s', updated_at='%s' WHERE queue='%s'
	`
	SelectQueueByStatusStmt = `
		SELECT queue, status, concurrent from %s where status='%s'
	`
	SelectQueueByNameStmt = `
		SELECT queue, status, concurrent from %s where queue='%s' limit 1
	`
)

type DB struct {
	*sql.DB
}

type QueueMSGRecord struct {
	Id         string `db:"id"`
	CreatedAt  string `db:"created_at"`
	UpdatedAt  int    `db:"updated_at"`
	Status     string `db:"status"`
	Queue      string `db:"queue"`
	ConsumerId string `db:"consumer_id"`
	Data       string `db:"data"`
}

type QueueRecord struct {
	Id         string `db:"id"`
	CreatedAt  string `db:"created_at"`
	UpdatedAt  string `db:"updated_at"`
	Status     string `db:"status"`
	Queue      string `db:"queue"`
	Concurrent int    `db:"concurrent"`
}

func NewDB(c *Config) (*DB, error) {
	// c := &Config{
	// 	&DefaultDBOption,
	// 	&DefaultMQOption,
	// }
	// for _, opt := range opts {
	// 	opt(c)
	// }
	sqlDb, _ := sql.Open("mysql", c.DSN)

	if err := sqlDb.Ping(); err != nil {
		return nil, err
	}

	db := &DB{sqlDb}
	db.SetMaxIdleConns(c.MaxIdleConns)
	db.SetMaxOpenConns(c.MaxOpenConns)
	db.SetConnMaxLifetime(c.ConnMaxLifetime)

	db.createTabel(c)
	return db, nil
}

func (d *DB) createTabel(c *Config) error {
	createQueue := fmt.Sprintf(CreateQueueTableStmt, c.QueueTable)

	createMessage := fmt.Sprintf(CreateMessageTableStmt, c.MsgQueueTable)

	if _, err := d.Exec(createQueue); err != nil {
		return err
	}
	if _, err := d.Exec(createMessage); err != nil {
		return err
	}
	return nil
}
