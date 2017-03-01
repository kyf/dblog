package dblog

import (
	"time"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type DBLog struct {
	sess           *mgo.Session
	ch             chan interface{}
	logger         Logger
	db, collection string
}

type Logger interface {
	Print(...interface{})

	Printf(string, ...interface{})

	Error(...interface{})

	Errorf(string, ...interface{})

	Fatal(...interface{})
}

func New(host, user, password, _db, _collection string, ch chan interface{}, logger Logger) (*DBLog, error) {
	sess, err := mgo.Dial(host)
	if err != nil {
		return nil, err
	}

	if user != "" && password != "" {
		credential := &mgo.Credential{Username: user, Password: password, Source: "admin"}
		err = sess.Login(credential)
		if err != nil {
			return nil, err
		}
	}

	dblog := &DBLog{sess: sess, ch: ch, logger: logger, db: _db, collection: _collection}
	go dblog.write()
	return dblog, nil
}

func (this *DBLog) write() {
	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := this.sess.Ping(); err != nil {
				this.sess.Refresh()
			}
		case it := <-this.ch:
			if err := this.sess.DB(this.db).C(this.collection).Insert(it); err != nil {
				this.logger.Errorf("dblog:  write err :%v", err)
			}
		}
	}
}

func (this *DBLog) Update(condition, update bson.M) error {
	return this.sess.DB(this.db).C(this.collection).Update(condition, update)
}

func (this *DBLog) Read(condition bson.M, page, size int, result interface{}) (int, error) {
	query := this.sess.DB(this.db).C(this.collection).Find(condition)
	count, err := query.Count()
	if err != nil {
		return 0, err
	}

	skip := (page - 1) * size
	err = query.Sort("-_id").Skip(skip).Limit(size).All(result)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (this *DBLog) Close() {
	if this.sess != nil {
		this.sess.Close()
	}
}
