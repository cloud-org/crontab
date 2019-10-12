package main

import (
	"flag"
	"fmt"
	"github.com/ronething/golang-crontab/master"
	"runtime"
	"time"
)

var (
	confFile string
)

//解析命令行参数
func initArgs() {
	//	master -config ./master.json
	flag.StringVar(&confFile, "config", "./master.json", "指定 master.json")
	flag.Parse()
}

//初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	var (
		err error
	)
	// parse command args
	initArgs()
	//	初始化线程
	initEnv()

	// 加载配置
	if err = master.InitConfig(confFile); err != nil {
		goto ERR
	}

	// etcd 任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}

	// 启动 http 服务器
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	// 正常退出
	//return

ERR:
	fmt.Println(err)

}
