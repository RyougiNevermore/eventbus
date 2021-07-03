package eventbus_test

import (
	"fmt"
	"github.com/aacfactory/eventbus"
	"testing"
)

func TestMultiMap_Add(t *testing.T) {
	m := eventbus.MultiMap{}
	m.Add("a", "a")
	fmt.Println(m)
}
