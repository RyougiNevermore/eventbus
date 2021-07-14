package eventbus

import (
	"fmt"
	"github.com/valyala/bytebufferpool"
	"strings"
)

func tagsAddress(address string, tags []string) (key string) {
	key = address
	if tags != nil && len(tags) > 0 {
		tagsStr := tagsToString(tags)
		if tagsStr != "" {
			key = fmt.Sprintf("%s:%s", key, tagsStr)
		}
	}
	return
}

func parseTagsAddress(key string) (address string, tags []string) {
	if strings.Index(key, ":") < 0{
		address = key
		tags = make([]string, 0, 1)
		return
	}
	keys := strings.Split(key, ":")
	address = keys[0]
	tags = tagsClean(strings.Split(keys[1], "-"))
	return
}

func tagsFromDeliveryOptions(options ...DeliveryOptions) (tags []string) {
	if options == nil || len(options) == 0 {
		return
	}
	tags = make([]string, 0, 1)
	for _, option := range options {
		tags0, has := option.Values("tag")
		if has {
			for _, tag := range tags0 {
				tags = append(tags, strings.TrimSpace(tag))
			}
		}
	}
	tags = tagsClean(tags)
	return
}

func tagsClean(tags []string) (cleanedTags []string) {
	if tags == nil {
		return
	}
	cleanedTags = make([]string, 0, 1)
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		contains := false
		for _, cleanedTag := range cleanedTags {
			if cleanedTag == tag {
				contains = true
				break
			}
		}
		if contains {
			continue
		}
		cleanedTags = append(cleanedTags, tag)
	}
	return
}

func tagsToString(tags []string) (v string) {
	bb := bytebufferpool.Get()
	defer bytebufferpool.Put(bb)
	for _, tag := range tags {
		_, _ = bb.WriteString(tag)
		_, _ = bb.WriteString("-")
	}
	v = bb.String()
	if len(v) > 0 {
		v = v[:len(v)-1]
	}
	return
}
