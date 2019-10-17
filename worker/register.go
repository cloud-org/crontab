package worker

import (
	"context"
	"github.com/ronething/golang-crontab/common"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"net"
	"time"
)

// 注册节点到 etcd /cron/workers/{IP}
type Register struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	localIP string
}

var (
	G_register *Register
)

func InitRegister() (err error) {

	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		localIP string
	)

	if localIP, err = getLocalIP(); err != nil {
		return
	}

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

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIP,
	}

	go G_register.keepOnline()

	return

}

// 获取本机非 lo 网卡的 ip
func getLocalIP() (ipv4 string, err error) {
	var (
		addrs   []net.Addr
		addr    net.Addr
		ipNet   *net.IPNet
		isIpNet bool
	)

	// 遍历所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}

	// 取第一个非 lo 的网卡 IP
	for _, addr = range addrs {
		// ip, unix
		if ipNet, isIpNet = addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = common.ErrNoLocalIPFound
	return

}

// 注册到 /cron/workers/{IP}, 并自动续租
func (r *Register) keepOnline() {
	var (
		regKey         string
		leaseGrantResp *clientv3.LeaseGrantResponse
		keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp  *clientv3.LeaseKeepAliveResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
		err            error
	)

	for {

		cancelFunc = nil
		regKey = common.JobWorkerDir + r.localIP

		if leaseGrantResp, err = r.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		// 自动续租
		if keepAliveChan, err = r.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		if _, err = r.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				if keepAliveResp == nil {
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}

}
