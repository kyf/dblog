package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/kyf/dblog"
	mlog "github.com/kyf/util/log"
)

const (
	LOG_DIR    = "./"
	LOG_PREFIX = "[dblog_test]"
)

type Person struct {
	Name string `json:"name" bson:"name"`
	Age  int    `json:"age" bson:"age"`
}

func main() {
	logger, err := mlog.NewLogger(LOG_DIR, LOG_PREFIX, log.LstdFlags)
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Close()

	ch1 := make(chan interface{}, 10)
	dblog1, err := dblog.New("127.0.0.1:3717", "root", "6renyou", "test", "test", ch1, logger)
	if err != nil {
		logger.Fatal(err)
	}
	defer dblog1.Close()

	ch2 := make(chan interface{}, 10)
	dblog2, err := dblog.New("127.0.0.1:27017", "", "", "test", "test", ch2, logger)
	if err != nil {
		logger.Fatal(err)
	}
	defer dblog2.Close()

	go func() {
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				person := Person{Name: fmt.Sprintf("name_%d", time.Now().UnixNano()), Age: 100}
				ch1 <- person
				ch2 <- person
			}
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		page, size := params.Get("page"), params.Get("size")

		_page, _size := 1, 10
		if page != "" {
			_page, _ = strconv.Atoi(page)
		}

		if size != "" {
			_size, _ = strconv.Atoi(size)
		}

		if _page < 1 {
			_page = 1
		}

		if _size < 1 {
			_size = 10
		}

		var result []Person
		count, err := dblog2.Read(nil, _page, _size, &result)
		if err != nil {
			logger.Errorf("dblog2.Read err:%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		jsonresult, err := json.Marshal(result)
		if err != nil {
			logger.Errorf("json.Marshal err:%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Print("count is ", count)

		w.Write([]byte(jsonresult))
	})

	log.Fatal(http.ListenAndServe(":1212", nil))
}
