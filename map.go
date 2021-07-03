package eventbus

type MultiMap map[string][]string

func (h MultiMap) Add(key string, value string) {
	h[key] = append(h[key], value)
}

func (h MultiMap) Put(key string, value []string) {
	h[key] = value
}

func (h MultiMap) Get(key string) (string, bool) {
	if h == nil {
		return "", false
	}
	if v, has := h[key]; has && v != nil {
		return v[0], true
	}
	return "", false
}

func (h MultiMap) Values(key string) ([]string, bool) {
	v, has := h[key]
	return v, has
}

func (h MultiMap) Remove(key string) {
	delete(h, key)
}

func (h MultiMap) Keys() []string {
	if h.Empty() {
		return nil
	}
	keys := make([]string, 0, 1)
	for key := range h {
		keys = append(keys, key)
	}
	return keys
}

func (h MultiMap) Empty() bool {
	return h == nil || len(h) == 0
}

func (h MultiMap) Merge(o ...MultiMap) {
	if o == nil || len(o) == 0 {
		return
	}
	for _, other := range o {
		for k, v := range other {
			ov, _ := h.Values(k)
			if ov == nil {
				ov = make([]string, 0, 1)
			}
			for i := 0; i < len(v); i++ {
				ov = append(ov, v[i])
			}
			h.Put(k, ov)
		}
	}
}
