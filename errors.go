package eventbus

import (
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/valyala/bytebufferpool"
)

func newReplyErr(err error) errors.CodeError {
	return &ReplyError{
		Id:          "",
		FailureCode: 500,
		Code:        "***SERVICE EXECUTE FAILED***",
		Message:     err.Error(),
		Meta:        errors.MultiMap{},
	}
}

type ReplyError struct {
	Id          string          `json:"id,omitempty"`
	FailureCode int             `json:"failureCode,omitempty"`
	Code        string          `json:"code,omitempty"`
	Message     string          `json:"message,omitempty"`
	Meta        errors.MultiMap `json:"meta,omitempty"`
}

func (e *ReplyError) SetId(id string) errors.CodeError {
	e.Id = id
	return e
}

func (e *ReplyError) SetFailureCode(failureCode int) errors.CodeError {
	e.FailureCode = failureCode
	return e
}

func (e *ReplyError) GetMeta() errors.MultiMap {
	return e.Meta
}

func (e *ReplyError) GetStacktrace() (fn string, file string, line int) {
	fn = "unknown"
	file = "unknown"
	line = 0
	return
}

func (e *ReplyError) Error() string {
	return e.String()
}

func (e *ReplyError) String() string {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)
	_, _ = bb.WriteString("\n")
	if e.Id != "" {
		_, _ = bb.WriteString(fmt.Sprintf("ID      = [%s]\n", e.Id))
	}
	_, _ = bb.WriteString(fmt.Sprintf("CODE    = [%d][%s]\n", e.FailureCode, e.Code))
	_, _ = bb.WriteString(fmt.Sprintf("MESSAGE = %s\n", e.Message))
	if !e.Meta.Empty() {
		_, _ = bb.WriteString("META    = ")
		for i, key := range e.Meta.Keys() {
			values, _ := e.Meta.Values(key)
			if i == 0 {
				_, _ = bb.WriteString(fmt.Sprintf("%s : %v\n", key, values))
			} else {
				_, _ = bb.WriteString(fmt.Sprintf("          %s : %v\n", key, values))
			}
		}
	}
	_, _ = bb.WriteString(fmt.Sprintf("STACK   = %s %s:%d\n", "unknown", "unknown", 0))

	return string(bb.Bytes()[:bb.Len()-1])
}

func (e *ReplyError) ToJson() []byte {
	return jsonEncode(e)
}
