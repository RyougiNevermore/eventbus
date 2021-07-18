package eventbus

import "os"

func hostname() (name string) {
	var has bool
	name, has = os.LookupEnv("HOSTNAME")
	if !has {
		name, _ = os.Hostname()
	}
	return
}
