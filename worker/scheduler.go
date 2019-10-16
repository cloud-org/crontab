package worker

import (
	"fmt"
	"github.com/ronething/golang-crontab/common"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent              // etcd 任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan // 任务调度计划表 与 etcd 一致
	jobExecutingTable map[string]*common.JobExecuteInfo  // 任务执行表
	jobResultChan     chan *common.JobExecuteResult      // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// 处理任务时间
func (s *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		jobExisted      bool
		jobExecuteInfo  *common.JobExecuteInfo
		jobExecuting    bool
		err             error
	)
	switch jobEvent.EventType {
	case common.JobEventSave:
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job); err != nil {
			return
		}
		s.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	case common.JobEventDelete:
		if jobSchedulePlan, jobExisted = s.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(s.jobPlanTable, jobEvent.Job.Name)
		}
	case common.JobEventKill:
		//	强制取消任务执行
		if jobExecuteInfo, jobExecuting = s.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.KillJob()
		}

	}

}

// 尝试执行任务
// 调度和执行是两件事情，
// 执行的任务可能会很久，例如定时任务 */1 * * * * *，1 分钟调度 60 次，但是只能执行一次，
// 通过 jobExecutingTable 防止并发
func (s *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) () {
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)

	// 如果任务正在执行，跳过本次调度
	if jobExecuteInfo, jobExecuting = s.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("任务正在执行", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BuildJobExecuteInfo(jobPlan)

	//保存执行状态
	s.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("开始执行任务", jobExecuteInfo.Job.Name, jobExecuteInfo.PlanTime, jobExecuteInfo.RealTime)
	G_executor.ExecuteJob(jobExecuteInfo)

}

// 重新计算任务调度状态
func (s *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		jobPlan  *common.JobSchedulePlan
		now      time.Time
		nearTime *time.Time
	)

	// 如果任务表为空 则睡一秒
	if len(s.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}

	now = time.Now()

	// 1、遍历所有任务
	for _, jobPlan = range s.jobPlanTable {
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// 尝试执行任务(如果上一次任务还没有执行完成，则此次不执行)
			s.TryStartJob(jobPlan)
			jobPlan.NextTime = jobPlan.Expr.Next(now) // 更新下次执行时间
		}
		// 统计最近的要过期的任务的时间 (N 秒后过期 == scheduleAfter)
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	if nearTime != nil {
		scheduleAfter = (*nearTime).Sub(now)
	}

	return

}

// 调度协程
func (s *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 初始化一次(因为第一次 table 中没有任务计划 所以这里其实是一秒)
	scheduleAfter = s.TrySchedule()

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	//	定时任务 commonJob
	for {
		select {
		case jobEvent = <-s.jobEventChan:
			// 对内存中维护的任务列表做增删改查
			s.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 最近的任务到期了
		case jobResult = <-s.jobResultChan:
			s.handleJobResult(jobResult)

		}

		// 两种 chan 推送过来都需要重新计算时间
		scheduleAfter = s.TrySchedule()
		// 重置定時器定时间隔
		scheduleTimer.Reset(scheduleAfter)
	}

}

//推送任务变化事件
func (s *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	s.jobEventChan <- jobEvent

}

func InitScheduler() (err error) {
	// 初始化 scheduler
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	// 启动调度协程
	go G_scheduler.scheduleLoop()

	return
}

// 回传任务执行结果
func (s *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	s.jobResultChan <- jobResult
}

// 处理任务结果
func (s *Scheduler) handleJobResult(jobExecuteResult *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(s.jobExecutingTable, jobExecuteResult.ExecuteInfo.Job.Name)

	// 上报日志
	if jobExecuteResult.Err != common.ErrLockAlreadyRequired {
		jobLog = &common.JobLog{
			JobName:      jobExecuteResult.ExecuteInfo.Job.Name,
			Command:      jobExecuteResult.ExecuteInfo.Job.Command,
			Output:       string(jobExecuteResult.Output),
			PlanTime:     jobExecuteResult.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: jobExecuteResult.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    jobExecuteResult.StartTime.UnixNano() / 1000 / 1000,
			EndTime:      jobExecuteResult.EndTime.UnixNano() / 1000 / 1000,
		}
		if jobExecuteResult.Err != nil {
			jobLog.Err = jobExecuteResult.Err.Error()
		} else {
			jobLog.Err = ""
		}
		G_logSink.Append(jobLog)
	}

	if jobExecuteResult.Err == nil {
		fmt.Println("任务执行完成", jobExecuteResult.ExecuteInfo.Job.Name, string(jobExecuteResult.Output))
	} else {
		fmt.Println("任务执行结束", jobExecuteResult.ExecuteInfo.Job.Name, jobExecuteResult.Err)
	}

}
