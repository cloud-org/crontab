package worker

import (
	"context"
	"fmt"
	"github.com/ronething/golang-crontab/common"
	"go.etcd.io/etcd/clientv3"
)

// 分布式锁 (txn 事务)
type JobLock struct {
	kv       clientv3.KV
	lease    clientv3.Lease
	jobName  string
	cancel   context.CancelFunc
	leaseId  clientv3.LeaseID
	isLocked bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}

	return
}

func (j *JobLock) TryLock() (err error) {
	var (
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId        clientv3.LeaseID
		keepRespChan   <-chan *clientv3.LeaseKeepAliveResponse
		ctx            context.Context
		cancel         context.CancelFunc
		txn            clientv3.Txn
		lockKey        string
		txnResp        *clientv3.TxnResponse
	)
	if leaseGrantResp, err = j.lease.Grant(context.TODO(), 5); err != nil {
		return
	}

	leaseId = leaseGrantResp.ID

	ctx, cancel = context.WithCancel(context.TODO())

	if keepRespChan, err = j.lease.KeepAlive(ctx, leaseId); err != nil {
		goto FAIL
	}

	go func() {
		var keepResp *clientv3.LeaseKeepAliveResponse
		for {
			select {
			case keepResp = <-keepRespChan:
				if keepResp == nil {
					// lease is done
					goto END
				} else {
					// lease continue
				}
			}
		}
	END:
	}()

	txn = j.kv.Txn(context.TODO())

	lockKey = common.JobLockDir + j.jobName

	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	if txnResp, err = txn.Commit(); err != nil {
		goto FAIL
	}

	//	判断是否抢到了锁
	if !txnResp.Succeeded {
		//err = errors.New(fmt.Sprintln("锁被占用", string(txnResp.Responses[0].GetResponseRange().Kvs[0].Key)))
		err = common.ErrLockAlreadyRequired
		goto FAIL
	}

	// 抢锁成功
	fmt.Println("抢锁成功", lockKey)
	j.cancel = cancel
	j.leaseId = leaseId
	j.isLocked = true

	return

FAIL:
	cancel()                                // 取消自动续约
	j.lease.Revoke(context.TODO(), leaseId) // 释放租约
	return
}

// 手动释放锁
func (j *JobLock) UnLock() {
	if j.isLocked {
		j.cancel()                                // 取消自动续约的协程
		j.lease.Revoke(context.TODO(), j.leaseId) // 释放租约
	}
}
