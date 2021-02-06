package master

import (
	"encoding/json"
	"fmt"
	"github.com/ronething/golang-crontab/common"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

// job's http interface
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

//init server
func InitApiServer() (err error) {
	var (
		mux           *http.ServeMux // 路由对象
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir
		staticHandler http.Handler
	)
	//		配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handleWorkerList)

	// 静态文件目录
	staticDir = http.Dir(G_config.Webroot)
	staticHandler = http.FileServer(staticDir)
	// strip / 之后 拼接上 staticDir 可以访问到 ./webroot/index.html
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		fmt.Println(err)
		return
	}

	//创建 http 服务
	httpServer = &http.Server{
		// 如果需要 debug 可以注释一下选项，这样才不会导致超时 (WriteTimeout)
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动服务端
	go httpServer.Serve(listener)

	return

}

// 获取健康 worker 节点列表
func handleWorkerList(w http.ResponseWriter, r *http.Request) {
	var (
		workerArr []string
		err       error
		resp      []byte
	)

	if workerArr, err = G_workerMgr.ListWorkers(); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", workerArr); err == nil {
		w.Write(resp)
	}

	return

ERR:
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}

}

// 展示日志
// GET /job/log jobName=j&skip=0&limit=10
func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobName string
		skip    int
		limit   int
		logArr  []*common.JobLog
		resp    []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	jobName = r.Form.Get("name")
	log.Printf("job name is %v\n", jobName)
	if skip, err = strconv.Atoi(r.Form.Get("skip")); err != nil {
		skip = 0
	}
	if limit, err = strconv.Atoi(r.Form.Get("limit")); err != nil {
		limit = 10
	}

	if logArr, err = G_logMgr.ListLog(jobName, skip, limit); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", logArr); err == nil {
		w.Write(resp)
	}

	return

ERR:
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
}

// 强杀一个任务
// POST /job/kill name=job1
func handleJobKill(w http.ResponseWriter, r *http.Request) {
	var (
		err  error
		name string
		resp []byte
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	if resp, err = common.BuildResponse(0, "success", name); err == nil {
		w.Write(resp)
	}

	return

ERR:
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
}

// 列举所有 job
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobList []*common.Job
		resp    []byte
	)
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(resp)
	}

	return

ERR:
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
}

// delete job
// POST /job/delete name=job1
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		oldJob *common.Job
		resp   []byte
	)

	if err = r.ParseForm(); err != nil {
		goto ERR
	}

	name = r.PostForm.Get("name")

	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	if resp, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(resp)
	}

	return

ERR:
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
}

// create or modify job
// POST job={"name":"job1","command":"echo hello","cronExpr":"* * * * *"}
func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		resp    []byte
	)
	//	1、解析 post 表单
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	//2、取表单中的 job 字段
	postJob = r.PostForm.Get("job")
	//3、反序列化 job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	//4、保存到 etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}

	//	5、返回正常应答
	if resp, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(resp)
	}

	return

ERR:
	//	6、返回异常应答
	if resp, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(resp)
	}
}
