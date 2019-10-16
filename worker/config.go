package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type Config struct {
	EtcdEndpoints         []string `json:"etcd_endpoints"`
	EtcdDialTimeout       int      `json:"etcd_dial_timeout"`
	MongodbUri            string   `json:"mongodb_uri"`
	MongodbConnectTimeout int      `json:"mongodb_connect_timeout"`
	JobLogBatchSize       int      `json:"job_log_batch_size"`
	JobLogCommitTimeout   int      `json:"job_log_commit_timeout"`
}

var (
	// 单例对象
	G_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)
	//	1、加载配置文件
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//2、做 json 反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3、赋值单例
	G_config = &conf

	fmt.Println(G_config)

	return

}
