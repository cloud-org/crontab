package main

import (
	"flag"
	"fmt"
	"github.com/ronething/golang-crontab/worker"
	"runtime"
	"time"
)

var (
	confFile string
)

//解析命令行参数
func initArgs() {
	//	worker -config ./master.json
	flag.StringVar(&confFile, "config", "./worker.json", "指定 worker.json")
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
	if err = worker.InitConfig(confFile); err != nil {
		goto ERR
	}

	// 启动执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}

	// 启动调度器 任务管理器依赖于它 所以先初始化
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}

	// 启动任务管理器
	if err = worker.InitJobMgr(); err != nil {
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
