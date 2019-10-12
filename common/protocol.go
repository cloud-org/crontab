package common

import "encoding/json"

// etcd 任务
type Job struct {
	Name     string `json:"name"`      // 任务名
	Command  string `json:"command"`   // shell 命令
	CronExpr string `json:"cron_expr"` // cron 表达式
}

// HTTP 接口应答
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var (
		response Response
	)
	response.Errno = errno
	response.Msg = msg
	response.Data = data

	if resp, err = json.Marshal(response); err != nil {
		return
	}

	return
}
