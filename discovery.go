package rpcxcli

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/smallnest/rpcx/client"
	"go.etcd.io/etcd/client/v3"
)

// Metadata 服务注册报文
// Schema 与 Addr 只是一个大致示例, 具体存放什么字段, 根据具体的服务注册与服务发现协商对应
type Metadata struct {
	Schema string `json:"schema"` // 协议
	Addr   string `json:"addr"`   // 地址
}

// discovery rpcx etcd 的服务发现
type discovery struct {
	cli    *clientv3.Client              // etcd client
	filter client.ServiceDiscoveryFilter // rpcx 服务发现的过滤器

	mutex    sync.RWMutex         // services 的读写锁
	services map[string]*Metadata // 有效的服务列表
	pairs    []*client.KVPair     // rpcx client 需要的服务列表

	cfg    clientv3.Config    // etcd client 的配置
	key    string             // 服务中心的 key 前缀
	ctx    context.Context    // ctx
	cancel context.CancelFunc // cancel
}

// Start 启动服务发现
func (d *discovery) Start() (err error) {
	if d.cli, err = clientv3.New(d.cfg); err != nil {
		return err
	}
	// 启动先拉取已有的服务
	if err = d.pull(); err != nil {
		return err
	}
	// 监测注册中心服务的变化
	go d.watch()

	return nil
}

// GetServices 获取所有的服务
func (d *discovery) GetServices() []*client.KVPair {
	//return []*client.KVPair{&client.KVPair{Key: "tcp@172.31.61.168:8082"}}
	return d.pairs
}

// WatchService 不明白什么用意
func (d *discovery) WatchService() chan []*client.KVPair {
	return nil
}

// RemoveWatcher 同上不明白什么用意
func (d *discovery) RemoveWatcher(ch chan []*client.KVPair) {
}

// Clone 克隆一个 ServiceDiscovery
func (d *discovery) Clone(svc string) (client.ServiceDiscovery, error) {
	return d, nil
}

// SetFilter 设置过滤器
func (d *discovery) SetFilter(filter client.ServiceDiscoveryFilter) {
	d.filter = filter
}

// Close 关闭服务发现
func (d *discovery) Close() {
	if d.cli != nil {
		_ = d.cli.Close()
	}
	d.cancel()
}

// pull 从 etcd 上拉取注册中心的全部服务
func (d *discovery) pull() error {
	res, err := d.cli.Get(d.ctx, d.key, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range res.Kvs {
		id := d.split(kv.Key)
		if id == "" {
			continue
		}
		md := &Metadata{}
		if err = json.Unmarshal(kv.Value, md); err != nil {
			continue
		}
		d.mutex.Lock()
		d.services[id] = md
		d.mutex.Unlock()
	}
	d.Paris()
	return nil
}

// watch 监控注册中心的服务变化
func (d *discovery) watch() {
	ch := d.cli.Watch(d.ctx, d.key, clientv3.WithPrefix())
	for res := range ch {
		// 新增删除
		for _, event := range res.Events {
			id := d.split(event.Kv.Key)
			if event.Type == clientv3.EventTypeDelete {
				d.mutex.Lock()
				delete(d.services, id)
				d.mutex.Unlock()
			} else {
				md := &Metadata{}
				if err := json.Unmarshal(event.Kv.Value, md); err != nil {
					continue
				}
				d.mutex.Lock()
				d.services[id] = md
				d.mutex.Unlock()
			}
		}

		d.Paris()
	}
}

// split 根据注册中心服务的 key 规则提取出服务的 id
func (d *discovery) split(key []byte) string {
	sp := strings.Split(string(key), d.key)
	if len(sp) != 2 {
		return ""
	}
	return sp[1]
}

// Paris 将服务列表整理成 rpcx 识别的服务列表
func (d *discovery) Paris() {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	d.pairs = d.pairs[:0]
	for _, md := range d.services {
		pair := &client.KVPair{Key: md.Addr, Value: md.Addr}
		if d.filter != nil && !d.filter(pair) {
			continue
		}
		d.pairs = append(d.pairs, pair)
	}
}
