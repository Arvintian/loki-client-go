package loki

import (
	"encoding/json"
	"time"

	"github.com/Arvintian/loki-client-go/pkg/logproto"
)

// batch holds pending log streams waiting to be sent to Loki, and it's used
// to reduce the number of push requests to Loki aggregating multiple log streams
// and entries in a single batch request. In case of multi-tenant Promtail, log
// streams for each tenant are stored in a dedicated batch.
type batch struct {
	streams   map[string]*logproto.Stream
	bytes     int
	createdAt time.Time
}

func newBatch(entries ...entry) *batch {
	b := &batch{
		streams:   map[string]*logproto.Stream{},
		bytes:     0,
		createdAt: time.Now(),
	}

	// Add entries to the batch
	for _, entry := range entries {
		b.add(entry)
	}

	return b
}

// add an entry to the batch
func (b *batch) add(entry entry) {
	b.bytes += len(entry.value[1])

	// Append the entry to an already existing stream (if any)
	labels := entry.labels.String()
	if stream, ok := b.streams[labels]; ok {
		stream.Values = append(stream.Values, entry.value)
		return
	}

	// Add the entry as a new stream
	b.streams[labels] = &logproto.Stream{
		Labels: entry.labels,
		Values: []logproto.Value{entry.value},
	}
}

// sizeBytes returns the current batch size in bytes
func (b *batch) sizeBytes() int {
	return b.bytes
}

// sizeBytesAfter returns the size of the batch after the input entry
// will be added to the batch itself
func (b *batch) sizeBytesAfter(entry entry) int {
	return b.bytes + len(entry.value)
}

// age of the batch since its creation
func (b *batch) age() time.Duration {
	return time.Since(b.createdAt)
}

// encode the batch as json push request, and returns
// the encoded bytes and the number of encoded entries
func (b *batch) encodeJSON() ([]byte, int, error) {
	req, entriesCount := b.createPushRequest()
	buf, err := json.Marshal(req)
	if err != nil {
		return nil, 0, err
	}
	return buf, entriesCount, nil
}

// creates push request and returns it, together with number of entries
func (b *batch) createPushRequest() (*logproto.PushRequest, int) {
	req := logproto.PushRequest{
		Streams: make([]logproto.Stream, 0, len(b.streams)),
	}

	entriesCount := 0
	for _, stream := range b.streams {
		req.Streams = append(req.Streams, *stream)
		entriesCount += len(stream.Values)
	}
	return &req, entriesCount
}
