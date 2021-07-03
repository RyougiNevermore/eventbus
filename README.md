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

func HandlerReply(head eventbus.MultiMap, body []byte) (result interface{}, err error) {
    arg := &Arg{}
    _ = json.Unmarshal(body, arg)
    fmt.Println("handle reply", head, arg)
    if arg.Num < 0 {
        err = errors.InvalidArgumentErrorWithDetails("bad number", "num", "less than 0")
        return
    }
    result = &Result{
        Value: "result",
    }
    return
}

func HandlerVoid(head eventbus.MultiMap, body []byte) (result interface{}, err error) {
    arg := &Arg{}
    _ = json.Unmarshal(body, arg)
    fmt.Println("handle void", head, arg)
    return
}

_ = eb.RegisterHandler("void", HandlerVoid)
_ = eb.RegisterHandler("reply", HandlerReply)
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
    requestErr := rf.Result(result)
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
```go
// todo
```
### NATS 模型
```go
// todo
```