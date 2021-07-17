package eventbus

import (
	"github.com/aacfactory/errors"
)

func newReplyErr(err error) errors.CodeError {
	return errors.Map(err)
}
