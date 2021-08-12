package rpcxcli

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/smallnest/rpcx/client"
	"go.etcd.io/etcd/client/v3"
)

type Metadata struct {
	Addr string
}

type discovery struct {
	cli    *clientv3.Client
	filter client.ServiceDiscoveryFilter

	mutex    sync.RWMutex
	services map[string]*Metadata
	pairs    []*client.KVPair

	cfg    clientv3.Config
	key    string
	ctx    context.Context
	cancel context.CancelFunc
}

// Start 启动服务发现
func (d *discovery) Start() (err error) {
	if d.cli, err = clientv3.New(d.cfg); err != nil {
		return err
	}
	_ = d.pull()
	go d.watch()
	return nil
}

// GetServices 获取所有的服务
func (d *discovery) GetServices() []*client.KVPair {
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

func (d *discovery) SetFilter(filter client.ServiceDiscoveryFilter) {
	d.filter = filter
}

func (d *discovery) Close() {
	if d.cli != nil {
		_ = d.cli.Close()
	}
	d.cancel()
}

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

func (d *discovery) split(key []byte) string {
	sp := strings.Split(string(key), d.key)
	if len(sp) != 2 {
		return ""
	}
	return sp[1]
}

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
