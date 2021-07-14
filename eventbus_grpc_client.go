package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/eventbus/internal"
	"github.com/dgraph-io/ristretto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"sync/atomic"
)

const (
	ebsGrpcSchema = "ebs"
	defaultGrpcClientCacheNumCounters = 128 * (1 << 20) / 100
	defaultGrpcClientCacheMaxCost     = 128 * (1 << 20)
)

func newGrpcEventbusClient(discovery ServiceDiscovery) (client *grpcEventbusClient, err error) {

	client = &grpcEventbusClient{
		running:       0,
		discovery:     discovery,
		connCachedMap: nil,
	}

	cache, newCacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters: defaultGrpcClientCacheNumCounters,
		MaxCost:     defaultGrpcClientCacheMaxCost,
		BufferItems: 64,
		OnEvict:     client.onEvictConn,
	})

	if newCacheErr != nil {
		err = fmt.Errorf("eventbus new cluster client cache failed, %v", newCacheErr)
		return
	}

	client.connCachedMap = cache

	resolver.Register(&ebsResolverBuilder{
		discovery: discovery,
	})

	return
}

type grpcEventbusClient struct {
	running       int64
	discovery     ServiceDiscovery
	connCachedMap *ristretto.Cache
}

func (client *grpcEventbusClient) Send(msg *message) (err error) {
	if client.closed() {
		err = fmt.Errorf("eventbus send failed, client is closed")
		return
	}
	address, hasAddress := msg.getAddress()
	if !hasAddress {
		err = fmt.Errorf("eventbus send failed, address is empty")
		return
	}
	tags, _ := msg.getTags()

	conn, has, getErr := client.getConn(address, tags)
	if getErr != nil {
		err = fmt.Errorf("eventbus send failed, get address handler failed, %v", getErr)
		return
	}
	if !has {
		err = fmt.Errorf("eventbus request failed, handler of address is not found")
		return
	}
	grpcClient := internal.NewEventbusClient(conn)
	remoteMsg := &internal.Message{
		Header: jsonEncode(msg.Head),
		Body:   nil,
	}
	if msg.Body != nil && len(msg.Body) > 0 {
		remoteMsg.Body = msg.Body
	}
	_, sendErr := grpcClient.Send(context.TODO(), remoteMsg)
	if sendErr != nil {
		err = fmt.Errorf("eventbus send failed, %v", sendErr)
		return
	}

	return
}

func (client *grpcEventbusClient) Request(msg *message) (reply *ReplyFuture) {
	if client.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, client is closed"))
		return
	}
	address, hasAddress := msg.getAddress()
	if !hasAddress {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}
	tags, _ := msg.getTags()

	conn, has, getErr := client.getConn(address, tags)
	if getErr != nil {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, get address handler failed, %v", getErr))
		return
	}
	if !has {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, handler of address is not found"))
		return
	}
	grpcClient := internal.NewEventbusClient(conn)
	remoteMsg := &internal.Message{
		Header: jsonEncode(msg.Head),
		Body:   nil,
	}
	if msg.Body != nil && len(msg.Body) > 0 {
		remoteMsg.Body = msg.Body
	}
	result, requestErr := grpcClient.Request(context.TODO(), remoteMsg)
	if requestErr != nil {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, %v", requestErr))
		return
	}

	if result == nil {
		reply = newSucceedFuture(&message{})
		return
	}
	replyMsg, replyMsgErr := newMessageFromBytes(result.Header, result.Body)
	if replyMsgErr != nil {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, parse result failed, %v", replyMsgErr))
		return
	}
	reply = newSucceedFuture(replyMsg)

	return
}

func (client *grpcEventbusClient) Start() {
	atomic.StoreInt64(&client.running, int64(1))
}

func (client *grpcEventbusClient) Close() {
	atomic.StoreInt64(&client.running, int64(0))
	client.connCachedMap.Clear()
}

func (client *grpcEventbusClient) closed() bool {
	return atomic.LoadInt64(&client.running) == int64(0)
}

func (client *grpcEventbusClient) getConn(address string, tags []string) (conn *grpc.ClientConn, has bool, err error) {
	if address == "" {
		err = fmt.Errorf("eventbus get or create client failed, address is empty")
		return
	}
	key := tagsAddress(address, tags)
	conn0, cached := client.connCachedMap.Get(key)
	if cached {
		conn, has = conn0.(*grpc.ClientConn)
		return
	}
	conn1, dailErr := client.dial(key)
	if dailErr != nil {
		err = fmt.Errorf("eventbus get or create client failed for dial failed, address is %s, %v", address, dailErr)
	}
	client.connCachedMap.Set(key, conn1, 1)
	client.connCachedMap.Wait()
	conn = conn1
	has = true
	return
}

func (client *grpcEventbusClient) dial(key string) (conn *grpc.ClientConn, err error) {

	address, tags := parseTagsAddress(key)

	registration, has, getErr := client.discovery.Get(defaultGroupName, address, tags...)
	if getErr != nil {
		err = fmt.Errorf("get %s handler from discovery failed, %s", address, getErr)
		return
	}
	if !has {
		err = fmt.Errorf("%s handler is not founed", address)
		return
	}
	registrationTLS := registration.TLS()
	dialOptions := make([]grpc.DialOption, 0, 1)
	dialOptions = append(dialOptions,
		grpc.WithBlock(),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if registrationTLS != nil && registrationTLS.Enable() {
		clientTLSConfig, tlsErr := registrationTLS.ToClientTLSConfig()
		if tlsErr != nil {
			err = fmt.Errorf("create %s client failed for build tls, %s", address, tlsErr)
			return
		}
		dialOptions = append(dialOptions,
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSConfig)),
		)
	} else {
		dialOptions = append(dialOptions,
			grpc.WithInsecure(),
		)
	}
	conn0, dialErr := grpc.Dial(
		fmt.Sprintf("%s:///%s", ebsGrpcSchema, key),
		dialOptions...,
	)
	if dialErr != nil {
		err = fmt.Errorf("dial %s failed, %s", address, dialErr)
		return
	}
	conn = conn0

	return
}

func (client *grpcEventbusClient) onEvictConn(item *ristretto.Item) {
	conn, ok := item.Value.(*grpc.ClientConn)
	if !ok {
		return
	}
	_ = conn.Close()
}

type ebsResolverBuilder struct {
	discovery ServiceDiscovery
}

func (b *ebsResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &ebsResolver{
		target:    target,
		cc:        cc,
		discovery: b.discovery,
	}

	r.ResolveNow(resolver.ResolveNowOptions{})

	return r, nil
}
func (*ebsResolverBuilder) Scheme() string { return ebsGrpcSchema }

type ebsResolver struct {
	target    resolver.Target
	cc        resolver.ClientConn
	discovery ServiceDiscovery
}

func (r *ebsResolver) ResolveNow(o resolver.ResolveNowOptions) {
	// todo target.endpoint -> address and tags
	address, tags := parseTagsAddress(r.target.Endpoint)
	registrations, has, getErr := r.discovery.GetALL(defaultGroupName, address, tags...)
	if getErr != nil {
		r.cc.ReportError(fmt.Errorf("get %s from discovery failed, %v", address, getErr))
		return
	}
	if !has {
		r.cc.ReportError(fmt.Errorf("get %s from discovery failed for not founed", address))
		return
	}

	addresses := make([]resolver.Address, 0, 1)
	for _, registration := range registrations {
		if registration.Status().Ok() {
			address := resolver.Address{
				Addr: registration.Address(),
			}
			addresses = append(addresses, address)
		}
	}
	if len(address) == 0 {
		r.cc.ReportError(fmt.Errorf("get %s from discovery failed for not founed", address))
		return
	}
	updateErr := r.cc.UpdateState(resolver.State{
		Addresses:     addresses,
		ServiceConfig: r.cc.ParseServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	})
	if updateErr != nil {
		r.cc.ReportError(fmt.Errorf("update %s state failed, %v", address, updateErr))
		return
	}
}

func (r *ebsResolver) Close() {

}
