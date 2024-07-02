package heartbeat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Config struct {
	Address  string        `yaml:"address"`
	Interval time.Duration `yaml:"interval"`
	NodeName string        `yaml:"nodeName"`
}

func Start(config Config) {
	timer := time.NewTicker(config.Interval * time.Second)
	defer timer.Stop()

	// 循环接收定时器的触发事件
	for range timer.C {
		// 执行发送 POST 请求的操作
		err := sendPostRequest(config)
		if err != nil {
			fmt.Println("send heartbeat post failed:", err)
		}
	}
}

func sendPostRequest(config Config) error {
	// fmt.Println("sending post request...")
	data := Config{
		NodeName: config.NodeName,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	body := bytes.NewBuffer(jsonData)

	resp, err := http.Post(config.Address, "application/json", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
