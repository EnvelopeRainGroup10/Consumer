package main

import (
	"Consumer/logger"
	"Consumer/sqlmodule"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"strconv"
	"strings"
)

func NewCreateEnvelopConsumer(t string, c string, addr string) error {
	conf := nsq.NewConfig()
	nc, err := nsq.NewConsumer(t, c, conf)
	if err != nil {
		logger.Logger.Error(err.Error())
		return err
	}

	nc.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		if string(msg.Body) == "hello" {
			return nil
		}
		parametersList := strings.Split(string(msg.Body), ",")
		envelopeId, _ := strconv.ParseInt(parametersList[0], 10, 64)
		uid, _ := strconv.ParseInt(parametersList[1], 10, 64)
		value, _ := strconv.ParseInt(parametersList[2], 10, 64)
		timeStamp, _ := strconv.ParseInt(parametersList[3], 10, 64)

		sqlmodule.CreateEnvelopeDetail(envelopeId, uid, value, timeStamp)
		return nil
	}))
	// 连接nsqlookupd
	if err:= nc.ConnectToNSQLookupd(addr);err!=nil{
		fmt.Println("connect nsqlookupd failed ", err)
		return err
	}
	return nil
}

func NewUpdateCountConsumer(t string, c string, addr string) error {
	conf := nsq.NewConfig()
	nc, err := nsq.NewConsumer(t, c, conf)
	if err != nil {
		fmt.Println("create consumer failed err ", err)
		return err
	}
	nc.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		if string(msg.Body) == "hello" {
			return nil
		}
		uid, err := strconv.ParseInt(string(msg.Body), 10, 64)
		if err != nil {
			log.Println(err)
		}
		sqlmodule.GetUser(uid)
		sqlmodule.UpdateCountByUid(uid)

		return nil
	}))

	// 连接nsqlookupd
	if err:= nc.ConnectToNSQLookupd(addr);err!=nil{
		fmt.Println("connect nsqlookupd failed ", err)
		return err
	}
	return nil
}

func NewUpdateStateConsumer(t string, c string, addr string) error {
	conf := nsq.NewConfig()
	nc, err := nsq.NewConsumer(t, c, conf)
	if err != nil {
		fmt.Println("create consumer failed err ", err)
		return err
	}

	nc.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		if string(msg.Body) == "hello" {
			return nil
		}
		parameterList := strings.Split(string(msg.Body), ",")
		envelopeId, _ := strconv.ParseInt(parameterList[0], 10, 64)
		uid, _ := strconv.ParseInt(parameterList[1], 10, 64)

		if err != nil {
			log.Println(err)
		}
		sqlmodule.UpdateStateByEidAndUid(envelopeId, uid)
		return nil
	}))

	// 连接nsqlookupd
	if err:= nc.ConnectToNSQLookupd(addr);err!=nil{
		fmt.Println("connect nsqlookupd failed ", err)
		return err
	}
	return nil
}

func main() {

	_, err := sqlmodule.InitDB()
	if err != nil {
		return
	}

	logger.InitLogger()

	go func() {
		addr := "111.62.107.178:4161"
		err := NewCreateEnvelopConsumer("CreateEnvelopeDetail", "channel-1", addr)
		if err != nil {
			fmt.Println("new nsq consumer failed", err)
			return
		}
		select {}
	}()

	go func() {
		addr := "111.62.107.178:4161"
		err := NewUpdateCountConsumer("UpdateCountByUid", "channel-1", addr)
		if err != nil {
			fmt.Println("new nsq consumer failed", err)
			return
		}
		select {}
	}()

	go func() {
		addr := "111.62.107.178:4161"
		err := NewUpdateStateConsumer("UpdateStateByEidAndUid", "channel-1", addr)
		if err != nil {
			fmt.Println("new nsq consumer failed", err)
			return
		}
		select {}
	}()

	select {}
}
