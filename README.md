# 概述
事件总线。使用事件驱动的方式进行业务解耦。<br/>
建议使用集群的方式运行，这样可以支持编程多语言环境。
# 获取
```go
go get github.com/aacfactory/eventbus
```
# 使用
## 本地
```go
// 构建
eb := eventbus.NewEventbus()
```
```go
// 挂载处理器

type Arg struct {
    Id       string    `json:"id,omitempty"`
    Num      int       `json:"num,omitempty"`
    Datetime time.Time `json:"datetime,omitempty"`
}

type Result struct {
    Value string `json:"value,omitempty"`
}

func HandlerReply(event eventbus.Event) (result interface{}, err error) {
    arg := &Arg{}
    _ = json.Unmarshal(event.Body(), arg)
    fmt.Println("handle reply", event.Head(), arg)
    if arg.Num < 0 {
        err = errors.InvalidArgumentErrorWithDetails("bad number", "num", "less than 0")
        return
    }
    result = &Result{
        Value: "result",
    }
    return
}

func HandlerVoid(event eventbus.Event) (result interface{}, err error) {
    arg := &Arg{}
    _ = json.Unmarshal(event.Body(), arg)
    fmt.Println("handle void", event.Head(), arg)
    return
}

_ = eb.RegisterHandler("void", HandlerVoid)
_ = eb.RegisterHandler("reply", HandlerReply)
```
```go
// 启动 （启动必须晚于挂载处理器）
eb.Start(context.TODO())
```
```go
// 执行
options := eventbus.NewDeliveryOptions()
options.Add("h1", "1")
options.Add("h2", "2")

sendErr := eb.Send("reply", &Arg{
    Id:       "id",
    Num:      10,
    Datetime: time.Now(),
}, options)

if sendErr != nil {
fmt.Println("send failed", sendErr)
}

for i := 0; i < 2; i++ {
    rf := eb.Request("reply", &Arg{
        Id:       "id",
        Num:      i - 1,
        Datetime: time.Now(),
    }, options)
    result := &Result{}
    requestErr := rf.Get(result)
    if requestErr != nil {
    	fmt.Println("request failed", requestErr)
    } else {
        fmt.Println("request succeed", result)
    }
}
```
```go
// 优雅的关闭
eb.Close(context.TODO())
```
## 集群
### TCP + 地址发现 模式
当前没有实现服务发现，请使用 [aacfactory/cluster](https://github.com/aacfactory/cluster) ，或自行实现。
```go
// options
options := eventbus.ClusterEventbusOption{
    Host:                       "0.0.0.0", // 实际监听地址
    Port:                       9090, // 实际监听端口
    PublicHost:                 "127.0.0.1", // 注册地址，如果为空，则默认使用监听地址
    PublicPort:                 0, // 注册端口，如果为空，则默认使用监听端口
    Meta:                       &eventbus.EndpointMeta{}, // 注册源数据
    Tags:                       nil, // 标签，一般用于版本化与运行隔离化
    TLS:                        &eventbus.EndpointTLS{}, // TLS 配置
    EventChanCap:               64, // 事件 chan 的长度
    Workers:                    8,  // 工作协程数量, 默认是CPU的2倍
}
// discovery
discovery := Foo{}
// 创建
bus, err = eventbus.NewClusterEventbus(discovery, options)
if err != nil {
    return
}
// 操作与本地Eventbus一样


```
### NATS 模型
```go
// options
options := eventbus.NatsEventbusOption{
        Name:                 "A", // nats 的 client 名称
        Servers:              []string{"nats://120.55.167.188:14222"},
        Username:             "ruser",
        Password:             "T0pS3cr3t",
        MaxReconnects:        10,
        ReconnectWaitSecond:  3,
        RetryOnFailedConnect: true,
        EventChanCap:         64,
}
// discovery
discovery := Foo{}
// 创建
bus, err = eventbus.NewNatsEventbus(discovery, options)
if err != nil {
    return
}
// 操作与本地Eventbus一样
```
