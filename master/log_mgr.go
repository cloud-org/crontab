package master

import (
	"context"
	"github.com/ronething/golang-crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

func InitLogMgr() (err error) {

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

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return

}

// 列出任务执行日志
func (l *LogMgr) ListLog(jobName string, skip int, limit int) (logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		findOpt *options.FindOptions
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)

	logArr = make([]*common.JobLog, 0)

	filter = &common.JobLogFilter{JobName: jobName}

	//	按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	findOpt = options.Find().SetSort(logSort).SetSkip(int64(skip)).SetLimit(int64(limit))
	if cursor, err = l.logCollection.Find(context.TODO(), filter, findOpt); err != nil {
		return
	}

	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}

		if err = cursor.Decode(jobLog); err != nil {
			// 日志不合法，忽略错误
			err = nil
			continue
		}

		logArr = append(logArr, jobLog)

	}

	return

}
