package eventbus

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"
)

var _json jsoniter.API

func initJsonApi() {
	_json = jsoniter.ConfigCompatibleWithStandardLibrary
}

func JSON() jsoniter.API {
	return _json
}

func JsonValid(data []byte) bool {
	return gjson.ValidBytes(data)
}

func JsonValidString(data string) bool {
	return gjson.Valid(data)
}

func JsonEncode(v interface{}) []byte {
	b, err := JSON().Marshal(v)
	if err != nil {
		panic(fmt.Errorf("json encode failed, target is %v, cause is %v", v, err))
	}
	return b
}

func JsonDecode(data []byte, v interface{}) {
	err := JSON().Unmarshal(data, v)
	if err != nil {
		panic(fmt.Errorf("json decode failed, target is %v, cause is %v", string(data), err))
	}
}

func JsonDecodeFromString(data string, v interface{}) {
	err := JSON().UnmarshalFromString(data, v)
	if err != nil {
		panic(fmt.Errorf("json decode from string failed, target is %v, cause is %v", data, err))
	}
}
