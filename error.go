package eventbus

import (
	"fmt"
	"github.com/pharosnet/errors"
	"github.com/tidwall/gjson"
	"github.com/valyala/bytebufferpool"
)

type ErrorStack struct {
	Fn   string `json:"fn"`
	File string `json:"file"`
	Line int    `json:"line"`
}

type CodeError struct {
	FailureCode int          `json:"failureCode"`
	Code        string       `json:"code,omitempty"`
	Message     string       `json:"message,omitempty"`
	Meta        MultiMap     `json:"meta,omitempty"`
	Stacktrace  []ErrorStack `json:"stacktrace,omitempty"`
}

func (e *CodeError) Error() string {
	return e.String()
}

func (e *CodeError) String() string {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)
	_, _ = bb.WriteString("\n")
	_, _ = bb.WriteString(fmt.Sprintf("CODE    = %s\n", e.Code))
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
	_, _ = bb.WriteString("STACK   = ")
	for i, stack := range e.Stacktrace {
		if i == 0 {
			_, _ = bb.WriteString(fmt.Sprintf("%s %s:%d\n", stack.Fn, stack.File, stack.Line))
		} else {
			_, _ = bb.WriteString(fmt.Sprintf("          %s %s:%d\n", stack.Fn, stack.File, stack.Line))
		}
	}
	return string(bb.Bytes()[:bb.Len()-1])
}

func NewCodeError(code string, message string) *CodeError {
	err := errors.NewWithDepth(1, 4, message)
	stacktrace := make([]ErrorStack, 0, 1)
	stackJsonField := gjson.Get(fmt.Sprintf("%-v", err), "stack")
	if stackJsonField.Exists() {
		decodeErr := JSON().UnmarshalFromString(stackJsonField.String(), &stacktrace)
		if decodeErr != nil {
			panic(fmt.Errorf("json decode failed, target is %v, cause is %v", stackJsonField.String(), decodeErr))
		}
	}
	return &CodeError{
		Code:       code,
		Message:    message,
		Meta:       MultiMap{},
		Stacktrace: stacktrace,
	}
}

func NewCodeErrorWithCause(code string, message string, cause error) *CodeError {
	err := errors.WithDepth(1, 4, cause, message)
	stacktrace := make([]ErrorStack, 0, 1)
	stackJsonField := gjson.Get(fmt.Sprintf("%-v", err), "stack")
	if stackJsonField.Exists() {
		decodeErr := JSON().UnmarshalFromString(stackJsonField.String(), &stacktrace)
		if decodeErr != nil {
			panic(fmt.Errorf("json decode failed, target is %v, cause is %v", stackJsonField.String(), decodeErr))
		}
	}
	return &CodeError{
		Code:       code,
		Message:    message,
		Meta:       MultiMap{},
		Stacktrace: stacktrace,
	}
}

func NewCodeErrorWithDepth(code string, message string, depth int) *CodeError {
	err := errors.NewWithDepth(depth, 3+depth, message)
	stacktrace := make([]ErrorStack, 0, 1)
	stackJsonField := gjson.Get(fmt.Sprintf("%-v", err), "stack")
	if stackJsonField.Exists() {
		decodeErr := JSON().UnmarshalFromString(stackJsonField.String(), &stacktrace)
		if decodeErr != nil {
			panic(fmt.Errorf("json decode failed, target is %v, cause is %v", stackJsonField.String(), decodeErr))
		}
	}
	return &CodeError{
		Code:       code,
		Message:    message,
		Meta:       MultiMap{},
		Stacktrace: stacktrace,
	}
}
