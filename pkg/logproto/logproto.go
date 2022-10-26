package logproto

import (
	"github.com/prometheus/common/model"
)

type PushRequest struct {
	Streams []Stream `json:"streams"`
}

type Stream struct {
	Labels model.LabelSet `json:"stream"`
	Values []Value        `json:"values"`
}

type Value [2]string
