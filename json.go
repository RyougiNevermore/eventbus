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

func jsonAPI() jsoniter.API {
	return _json
}

func jsonValid(data []byte) bool {
	return gjson.ValidBytes(data)
}

func jsonValidString(data string) bool {
	return gjson.Valid(data)
}

func jsonEncode(v interface{}) []byte {
	b, err := jsonAPI().Marshal(v)
	if err != nil {
		panic(fmt.Errorf("json encode failed, target is %v, cause is %v", v, err))
	}
	return b
}

func jsonDecode(data []byte, v interface{}) {
	err := jsonAPI().Unmarshal(data, v)
	if err != nil {
		panic(fmt.Errorf("json decode failed, target is %v, cause is %v", string(data), err))
	}
}

func jsonDecodeFromString(data string, v interface{}) {
	err := jsonAPI().UnmarshalFromString(data, v)
	if err != nil {
		panic(fmt.Errorf("json decode from string failed, target is %v, cause is %v", data, err))
	}
}
