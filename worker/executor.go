package worker

import (
	"bytes"
	"fmt"
	"github.com/ronething/golang-crontab/common"
	"os/exec"
	"syscall"
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
			b       bytes.Buffer
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
			cmd = exec.Command("/bin/bash", "-c", jobExecuteInfo.Job.Command)
			// 创建一个新的进程组
			cmd.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}

			cmd.Stdout = &b
			cmd.Stderr = &b

			if err = cmd.Start(); err != nil {
				goto DONE
			}

			jobExecuteInfo.Pid = cmd.Process.Pid

			if err = cmd.Wait(); err != nil {
				goto DONE
			}

			output = b.Bytes()

		DONE:
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
