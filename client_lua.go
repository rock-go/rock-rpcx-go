package rpcxcli

import (
	"reflect"
	"strings"

	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/node"
	"github.com/rock-go/rock/xcall"
	"github.com/rock-go/rock/xreflect"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	_TypeOfLuaClient = reflect.TypeOf((*luaClient)(nil)).String()
)

type luaConfig struct {
	Name     string `json:"name"     lua:"name"`     // lua 服务名称
	Service  string `json:"service"  lua:"service"`  // rpcx server 端服务名称
	Endpoint string `json:"endpoint" lua:"endpoint"` // etcd 注册中心地址
	Password string `json:"password" lua:"password"` // etcd 注册中心密码
}

type luaClient struct {
	lua.Super
	cli    *rpcxClient
	config *luaConfig
}

func LuaInjectApi(env xcall.Env) {
	kv := lua.NewUserKV()
	kv.Set("client", lua.NewFunction(newClient))
	env.SetGlobal("rpcx", kv)
}

func newClient(state *lua.LState) int {
	tbl := state.CheckTable(1)
	conf := &luaConfig{}

	if err := xreflect.ToStruct(tbl, conf); err != nil {
		state.RaiseError("%v", err)
		return 0
	}

	cli := &luaClient{config: conf}
	endpoint := strings.ReplaceAll(conf.Endpoint, " ", "")
	endpoints := strings.Split(endpoint, ",")
	cfg := clientv3.Config{Endpoints: endpoints, Username: node.ID(), Password: conf.Password}
	cli.cli = New(conf.Service, node.ID(), cfg)
	cli.S = lua.INIT

	proc := state.NewProc(conf.Name, _TypeOfLuaClient)
	proc.Value = cli
	state.Push(proc)

	return 1
}

func (c *luaClient) Start() error {
	if err := c.cli.Start(); err != nil {
		return err
	}
	c.S = lua.RUNNING
	return nil
}

func (c *luaClient) Close() error {
	if err := c.cli.Close(); err != nil {
		return err
	}
	c.S = lua.CLOSE
	return nil
}

func (c *luaClient) Call(state *lua.LState) int {
	top := state.GetTop()
	if top != 4 {
		state.RaiseError("%v", "参数错误, 需要 4 个参数")
		return 0
	}

	svc := state.CheckString(1)
	method := state.CheckString(2)
	args := state.CheckAnyData(3)
	reply := state.CheckAnyData(4)
	if err := c.cli.Call(svc, method, args.Value, reply.Value); err != nil {
		state.Push(lua.NewAnyData(err))
	} else {
		state.Push(lua.LNil)
	}
	return 1
}

// Index 注册方法
func (c *luaClient) Index(state *lua.LState, key string) lua.LValue {
	switch key {
	case "call":
		return lua.NewFunction(c.Call)
	}
	return lua.LNil
}
