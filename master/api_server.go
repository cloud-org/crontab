package master

import (
	"encoding/json"
	"fmt"
	"github.com/ronething/golang-crontab/common"
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
		mux        *http.ServeMux // 路由对象
		listener   net.Listener
		httpServer *http.Server
	)
	//		配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)

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
