package rpcxcli

import (
	"context"
	"errors"
	"github.com/smallnest/rpcx/share"

	"github.com/smallnest/rpcx/client"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type rpcxClient struct {
	rpcClient *client.OneClient
	cfg       clientv3.Config
	discovery client.ServiceDiscovery
	svc       string
	token     string

	ctx    context.Context
	cancel context.CancelFunc
}

// New 新建客户端
func New(svc, token string, cfg clientv3.Config) *rpcxClient {
	cli := &rpcxClient{cfg: cfg, svc: svc}
	cli.ctx, cli.cancel = context.WithCancel(context.Background())
	return cli
}

// Start 启动客户端
func (c *rpcxClient) Start() error {
	// 服务发现
	ctx, cancel := context.WithCancel(c.ctx)
	d := &discovery{ctx: ctx, cancel: cancel, key: "register/" + c.svc + "/", services: make(map[string]*Metadata, 8),
		pairs: make([]*client.KVPair, 0, 8), cfg: c.cfg}
	if err := d.Start(); err != nil {
		return err
	}
	c.rpcClient = client.NewOneClient(client.Failtry, client.RandomSelect, d, client.DefaultOption)
	c.rpcClient.Auth(c.token)
	return nil
}

// Close 关闭客户端
func (c *rpcxClient) Close() (err error) {
	if c.rpcClient != nil {
		err = c.rpcClient.Close()
	}
	return
}

// Call 调用服务
func (c *rpcxClient) Call(svc, method string, args, reply interface{}) error {
	if c.rpcClient == nil {
		return errors.New("client 未 start")
	}
	ctx := context.WithValue(context.Background(), share.ReqMetaDataKey, make(map[string]string))
	return c.rpcClient.Call(ctx, svc, method, args, reply)
}
