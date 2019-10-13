package common

const (
	// etcd 任务保存目录
	JobSaveDir = "/cron/jobs/"
	// 任务强杀目录
	JobKillerDir = "/cron/killer/"
	//	保存任务事件
	JobEventSave = 1
	//删除任务事件
	JobEventDelete = 2
)
