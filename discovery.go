package eventbus

//import (
//	"crypto/rand"
//	"crypto/tls"
//	"crypto/x509"
//	"fmt"
//)
//
//const (
//	EndpointStatusRunning = EndpointStatus("RUNNING")
//	EndpointStatusClosing = EndpointStatus("CLOSING")
//)
//
//type Status interface {
//	Ok() bool
//	Closing() bool
//}
//
//type Registration interface {
//	NodeId() (nodeId string)
//	NodeName() (nodeName string)
//	Id() (id string)
//	Group() (group string)
//	Name() (name string)
//	Status() (status Status)
//	Protocol() (protocol string)
//	Address() (address string)
//	Tags() (tags []string)
//	Meta() (meta Meta)
//	TLS() (registrationTLS RegistrationTLS)
//}
//
//type Meta interface {
//	Put(key string, value string)
//	Get(key string) (value string, has bool)
//	Rem(key string)
//	Keys() (keys []string)
//	Empty() (ok bool)
//	Merge(o ...Meta)
//}
//
//type RegistrationTLS interface {
//	Enable() bool
//	VerifySSL() bool
//	CA() string
//	ServerCert() string
//	ServerKey() string
//	ClientCert() string
//	ClientKey() string
//	ToServerTLSConfig() (config *tls.Config, err error)
//	ToClientTLSConfig() (config *tls.Config, err error)
//}
//
//type ServiceDiscovery interface {
//	Publish(group string, name string, protocol string, address string, tags []string, meta Meta, registrationTLS RegistrationTLS) (registration Registration, err error)
//	UnPublish(registration Registration) (err error)
//	Get(group string, name string, tags ...string) (registration Registration, has bool, err error)
//	GetALL(group string, name string, tags ...string) (registrations []Registration, has bool, err error)
//}
//
//func NewEndpointMeta() EndpointMeta {
//	return make(map[string]string)
//}
//
//type EndpointMeta map[string]string
//
//func (meta EndpointMeta) Put(key string, value string) {
//	meta[key] = value
//}
//
//func (meta EndpointMeta) Get(key string) (string, bool) {
//	if meta == nil {
//		return "", false
//	}
//	v, has := meta[key]
//	return v, has
//}
//
//func (meta EndpointMeta) Rem(key string) {
//	delete(meta, key)
//}
//
//func (meta EndpointMeta) Keys() []string {
//	if meta.Empty() {
//		return nil
//	}
//	keys := make([]string, 0, 1)
//	for key := range meta {
//		keys = append(keys, key)
//	}
//	return keys
//}
//
//func (meta EndpointMeta) Empty() bool {
//	return meta == nil || len(meta) == 0
//}
//
//func (meta EndpointMeta) Merge(o ...Meta) {
//	if o == nil || len(o) == 0 {
//		return
//	}
//	for _, other := range o {
//		keys := other.Keys()
//		for _, k := range keys {
//			v, has := other.Get(k)
//			if has {
//				meta.Put(k, v)
//			}
//		}
//	}
//}
//
//type EndpointTLS struct {
//	Enable_     bool   `json:"enable,omitempty"`
//	VerifySSL_  bool   `json:"verifySsl,omitempty"`
//	CA_         string `json:"ca,omitempty"`
//	ServerCert_ string `json:"serverCert,omitempty"`
//	ServerKey_  string `json:"serverKey,omitempty"`
//	ClientCert_ string `json:"clientCert,omitempty"`
//	ClientKey_  string `json:"clientKey,omitempty"`
//}
//
//func (s EndpointTLS) Enable() bool {
//	return s.Enable_
//}
//
//func (s EndpointTLS) VerifySSL() bool {
//	return s.VerifySSL_
//}
//
//func (s EndpointTLS) CA() string {
//	return s.CA_
//}
//
//func (s EndpointTLS) ServerCert() string {
//	return s.ServerCert_
//}
//
//func (s EndpointTLS) ServerKey() string {
//	return s.ServerKey_
//}
//
//func (s EndpointTLS) ClientCert() string {
//	return s.ClientCert_
//}
//
//func (s EndpointTLS) ClientKey() string {
//	return s.ClientKey_
//}
//
//func (s EndpointTLS) ToServerTLSConfig() (config *tls.Config, err error) {
//	if !s.Enable() {
//		err = fmt.Errorf("generate endpint server tls config failed, tls not enabled")
//		return
//	}
//	if s.ServerCert() == "" || s.ServerKey() == "" {
//		err = fmt.Errorf("generate endpint server tls config failed, key is empty")
//		return
//	}
//
//	certificate, certificateErr := tls.X509KeyPair([]byte(s.ServerCert()), []byte(s.ServerKey()))
//	if certificateErr != nil {
//		err = fmt.Errorf("generate endpint server tls config failed, %v", certificateErr)
//		return
//	}
//
//	config = &tls.Config{
//		Certificates:       []tls.Certificate{certificate},
//		Rand:               rand.Reader,
//		InsecureSkipVerify: !s.VerifySSL(),
//		ClientAuth:         tls.RequireAndVerifyClientCert,
//	}
//
//	if s.CA() != "" {
//		cas := x509.NewCertPool()
//		ok := cas.AppendCertsFromPEM([]byte(s.CA()))
//		if !ok {
//			err = fmt.Errorf("generate endpint server tls config failed, append ca failed")
//			return
//		}
//		config.ClientCAs = cas
//	}
//
//	return
//}
//
//func (s EndpointTLS) ToClientTLSConfig() (config *tls.Config, err error) {
//	if !s.Enable() {
//		err = fmt.Errorf("generate endpint client tls config failed, tls not enabled")
//		return
//	}
//	if s.ClientCert() == "" || s.ClientKey() == "" {
//		err = fmt.Errorf("generate endpint client tls config failed, key is empty")
//		return
//	}
//
//	certificate, certificateErr := tls.X509KeyPair([]byte(s.ClientCert()), []byte(s.ClientKey()))
//	if certificateErr != nil {
//		err = fmt.Errorf("generate endpint client tls config failed, %v", certificateErr)
//		return
//	}
//
//	config = &tls.Config{
//		Certificates:       []tls.Certificate{certificate},
//		InsecureSkipVerify: !s.VerifySSL(),
//	}
//
//	if s.CA() != "" {
//		cas := x509.NewCertPool()
//		ok := cas.AppendCertsFromPEM([]byte(s.CA()))
//		if !ok {
//			err = fmt.Errorf("generate endpint client tls config failed, append ca failed")
//			return
//		}
//		config.RootCAs = cas
//	}
//
//	return
//}
//
//type EndpointStatus string
//
//func (s EndpointStatus) Ok() bool {
//	return s == EndpointStatusRunning
//}
//
//func (s EndpointStatus) Closing() bool {
//	return s == EndpointStatusClosing
//}
