package eventbus

import (
	"fmt"
	"testing"
)

func TestNewCodeError(t *testing.T) {
	err := NewCodeError("***FOO***", "bar")
	err.Stacktrace = append(err.Stacktrace, ErrorStack{
		Fn:   "x",
		File: "x",
		Line: 1,
	})
	err.Meta.Add("a", "a")
	err.Meta.Put("b", nil)
	fmt.Println(err)
	fmt.Println(fmt.Sprintf("xxx %v", err))
	v , _ := JSON().Marshal(err)
	fmt.Println(string(v))
	//parser.ParseDir()
}
