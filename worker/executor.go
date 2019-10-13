package worker

import (
	"context"
	"github.com/ronething/golang-crontab/common"
	"os/exec"
	"time"
)

// 任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

// 执行一个任务
func (e *Executor) ExecuteJob(jobExecuteInfo *common.JobExecuteInfo) {

	go func() {
		var (
			cmd    *exec.Cmd
			err    error
			output []byte
			result *common.JobExecuteResult
		)

		result = &common.JobExecuteResult{
			ExecuteInfo: jobExecuteInfo,
			Output:      make([]byte, 0),
		}

		result.StartTime = time.Now()

		//	 执行 shell 命令
		cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", jobExecuteInfo.Job.Command)

		//	执行并捕获输出
		output, err = cmd.CombinedOutput()

		result.EndTime = time.Now()
		result.Output = output
		result.Err = err

		G_scheduler.PushJobResult(result)
	}()
}

func InitExecutor() (err error) {

	G_executor = &Executor{

	}

	return

}
