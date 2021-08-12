# rpcx client

```lua
local cli = rpcx.client {
    name = "服务名字"
    endpoint = "1.1.1.1:2379,1.1.1.2:2379"
    password = "xxxx"
}

proc.start(cli)

cli.call()
```

