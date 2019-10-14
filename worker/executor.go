package worker

import (
	"context"
	"fmt"
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
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)

		// 首先获取分布式锁
		jobLock = G_jobMgr.CreateJobLock(jobExecuteInfo.Job.Name)

		result = &common.JobExecuteResult{
			ExecuteInfo: jobExecuteInfo,
			Output:      make([]byte, 0),
		}

		result.StartTime = time.Now()

		err = jobLock.TryLock()
		defer jobLock.UnLock() // 最终需要释放锁

		if err != nil {
			fmt.Println(err)
			result.Err = err
			result.EndTime = time.Now()
		} else {
			// 上锁成功后，重置任务开始时间
			result.StartTime = time.Now()
			//	 执行 shell 命令
			cmd = exec.CommandContext(context.TODO(), "/bin/bash", "-c", jobExecuteInfo.Job.Command)

			//	执行并捕获输出
			output, err = cmd.CombinedOutput()

			result.EndTime = time.Now()
			result.Output = output
			result.Err = err

		}
		// 推送执行结果给调度协程
		G_scheduler.PushJobResult(result)

	}()
}

func InitExecutor() (err error) {

	G_executor = &Executor{

	}

	return

}
