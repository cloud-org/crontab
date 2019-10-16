package worker

import (
	"context"
	"github.com/ronething/golang-crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb 存储日志
type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var (
	G_logSink *LogSink
)

func (l *LogSink) saveLogs(batch *common.LogBatch) {
	l.logCollection.InsertMany(context.TODO(), batch.Logs)
}

func (l *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)

	for {
		select {
		case log = <-l.logChan:
			// 每次一条一条插入可能稍慢，
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				//	 超时自动提交
				commitTimer = time.AfterFunc(time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					func(batch *common.LogBatch) func() {
						return func() {
							l.autoCommitChan <- batch
						}
					}(logBatch))
			}
			logBatch.Logs = append(logBatch.Logs, log)

			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				l.saveLogs(logBatch)
				// clear logBatch
				logBatch = nil
				//	取消定时器
				commitTimer.Stop()
			}
		case timeoutBatch = <-l.autoCommitChan:
			// 如果定时器和 logBatchSize 同时进入 需要判断一下 batch 是否是原来的 batch
			if timeoutBatch != logBatch {
				continue
			}
			l.saveLogs(timeoutBatch)
			logBatch = nil
		}
	}
}

func InitLogSink() (err error) {
	var (
		client        *mongo.Client
		clientOptions *options.ClientOptions
	)

	// TODO: 不知为何这里设置 timeout 无法生效
	clientOptions = options.Client().ApplyURI(G_config.MongodbUri)
	clientOptions.SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	if client, err = mongo.Connect(context.TODO(), clientOptions); err != nil {
		return
	}

	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"),
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}

	go G_logSink.writeLoop()

	return
}

func (l *LogSink) Append(jobLog *common.JobLog) {
	// 缓冲区满直接跳过 后续可以进行优化
	select {
	case l.logChan <- jobLog:
	default:
	}
}
