package eventbus_test

import (
	"bufio"
	"io"
	"net"
	"testing"
	"time"
)

func TestTCP(t *testing.T)  {

	go func() {
		ln, lnErr := net.Listen("tcp", "127.0.0.1:9090")
		if lnErr != nil {
			t.Error(lnErr)
			return
		}

		go func(ln net.Listener) {
			time.Sleep(5 * time.Second)
			ln.Close()
		}(ln)

		for {
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				t.Error(acceptErr)
				break
			}
			go func() {
				time.Sleep(10 * time.Second)
				conn.Close()
			}()
			buf := bufio.NewReader(conn)
			for {
				head := make([]byte, 8)
				n, readErr := buf.Read(head)
				if readErr != nil {
					t.Log("read err", readErr)
					if readErr == io.EOF {
						t.Log("read eof")
						conn.Close()
					}
					break
				}
				if n == 0 {
					continue
				}
				t.Log("read", n, string(head[:n]), readErr)
				conn.Write([]byte("pong"))
			}

		}

	}()

	time.Sleep(2 * time.Second)

	conn, connErr := net.Dial("tcp", "127.0.0.1:9090")
	if connErr != nil {
		t.Error(connErr)
		return
	}
	time.Sleep(6 * time.Second)
	//r, rErr := conn.Read(make([]byte, 8))
	//t.Log("write - read", r, rErr)

	n, wErr := conn.Write([]byte(time.Now().String()))
	t.Log("write", n, wErr)
	r, rErr := conn.Read(make([]byte, 8))
	t.Log("write - read", r, rErr)
	conn.Close()
	time.Sleep(3* time.Second)
}
