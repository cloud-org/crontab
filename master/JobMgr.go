package master

import (
	"context"
	"encoding/json"
	"github.com/ronething/golang-crontab/common"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"time"
)

//任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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

	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return

}

func (jobMgr *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	//	把任务保存到 /cron/jobs/任务名 -> json 序列化
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)

	// 任务 key
	jobKey = "/cron/jobs/" + job.Name
	// 任务 value
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	if putResp, err = jobMgr.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
		return
	}
	//	 如果是更新，则返回旧值
	if putResp.PrevKv != nil {
		// 对旧值反序列化
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil // 忽略错误
			return
		}

		oldJob = &oldJobObj
	}

	return

}
