package worker

import (
	"context"
	"fmt"
	"github.com/ronething/golang-crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"google.golang.org/grpc"
	"time"
)

//任务管理器
type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	//	初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
		// 需要加上 DialOptions 不然不会有 err
		// more: https://github.com/etcd-io/etcd/issues/9877
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
	}

	if client, err = clientv3.New(config); err != nil {
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	G_jobMgr.watchJobs()

	return

}

// 监听任务变化
func (j *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvPair             *mvccpb.KeyValue
		job                *common.Job
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobName            string
		jobEvent           *common.JobEvent
	)
	//	1、获取 /cron/jobs/ 目录下的所有任务，并且获知当前集群的 revision
	if getResp, err = j.kv.Get(context.TODO(), common.JobSaveDir, clientv3.WithPrefix()); err != nil {
		return
	}
	// 获取当前任务
	for _, kvPair = range getResp.Kvs {
		if job, err = common.UnpackJob(kvPair.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JobEventSave, job)
			// 将任务同步给 scheduler(调度协程)
			fmt.Println(*jobEvent)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}
	//	2、从该 revision 向后监听变化事件
	go func() {
		//	监听协程
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = j.watcher.Watch(context.TODO(), common.JobSaveDir,
			clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: // 任务保存事件
					if job, err = common.UnpackJob(watchEvent.Kv.Value); err != nil {
						// 忽略错误
						err = nil
						continue
					}
					// 构造一个更新事件
					jobEvent = common.BuildJobEvent(common.JobEventSave, job)
					fmt.Println("save", *jobEvent)

				case mvccpb.DELETE: //	任务删除事件

					jobName = common.ExtractJobName(string(watchEvent.Kv.Key))

					job = &common.Job{
						Name: jobName,
					}

					jobEvent = common.BuildJobEvent(common.JobEventDelete, job)
					fmt.Println("delete", *jobEvent)

				}
				// 推送给 scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}

	}()

	return

}
