# golang-crontab

golang 实现分布式任务调度系统(依赖 etcd)

## 依赖

etcd、mongodb

### etcd

https://github.com/etcd-io/etcd

### mongodb

https://github.com/mongodb/mongo

## 架构

### master

### worker

## 效果

![master](https://i.loli.net/2019/10/19/lwDK9fnyzWmEoCG.png)

![command](https://i.loli.net/2019/10/19/nfsONJxTvMPaCFE.png)

## 致谢

https://yuerblog.cc

## TODO

- [ ] 任务超时控制
- [ ] 任务执行错误/超时进行告警
