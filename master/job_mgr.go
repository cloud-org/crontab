package master

import (
	"context"
	"encoding/json"
	"github.com/ronething/golang-crontab/common"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
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

// 新增或者修改任务
func (j *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	//	把任务保存到 /cron/jobs/任务名 -> json 序列化
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)

	// 任务 key
	jobKey = common.JobSaveDir + job.Name
	// 任务 value
	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	if putResp, err = j.kv.Put(context.TODO(), jobKey, string(jobValue), clientv3.WithPrevKV()); err != nil {
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

// 删除任务
func (j *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)

	// 任务 key
	jobKey = common.JobSaveDir + name
	// 任务 value
	if delResp, err = j.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	//	 返回被删除的任务信息
	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

// 列出 etcd 所有任务
func (j *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)

	dirKey = common.JobSaveDir

	if getResp, err = j.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}

	// 初始化数组空间 默认为 nil
	jobList = make([]*common.Job, 0)

	// 遍历任务 进行反序列化
	for _, kvPair = range getResp.Kvs {
		// 需要初始化 不然会有空指针错误 并且需要在内部初始化 不然最终会因为指向地址相同导致数据相同
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue // 忽略错误
		}
		jobList = append(jobList, job)
	}

	return
}

// 杀死任务
// 更新 /cron/killer/任务名，worker watch 到则杀死
func (j *JobMgr) KillJob(name string) (err error) {
	var (
		killerKey      string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
	)

	killerKey = common.JobKillerDir + name

	//	让 worker 监听到一次 put 操作 并通过租约删除 (不占用内存)
	if leaseGrantResp, err = j.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	if _, err = j.kv.Put(context.TODO(), killerKey, "", clientv3.WithLease(leaseId)); err != nil {
		return
	}

	return

}
