package loki

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Arvintian/loki-client-go/pkg/backoff"

	"github.com/Arvintian/loki-client-go/pkg/httpconfig"
	"github.com/Arvintian/loki-client-go/pkg/model"

	"github.com/Arvintian/loki-client-go/pkg/helpers"
	"github.com/Arvintian/loki-client-go/pkg/logproto"
)

const (
	protoContentType = "application/x-protobuf"
	JSONContentType  = "application/json"
	maxErrMsgLen     = 1024

	// Label reserved to override the tenant ID while processing
	// pipeline stages
	ReservedLabelTenantID = "__tenant_id__"

	Version = "v0.0.2"
)

var (
	UserAgent = fmt.Sprintf("LokiGoClient/%s", Version)
)

// Client for pushing logs in snappy-compressed protos over HTTP.
type Client struct {
	cfg     Config
	client  *http.Client
	quit    chan struct{}
	once    sync.Once
	entries chan entry
	wg      sync.WaitGroup

	externalLabels model.LabelSet
}

type entry struct {
	tenantID string
	labels   model.LabelSet
	value    logproto.Value
}

// NewWithDefault creates a new client with default configuration.
func NewWithDefault(url string) (*Client, error) {
	cfg, err := NewDefaultConfig(url)
	if err != nil {
		return nil, err
	}
	return New(cfg)
}

// New makes a new Client from a config
func New(cfg Config) (*Client, error) {
	if cfg.URL.URL == nil {
		return nil, errors.New("client needs target URL")
	}

	c := &Client{
		cfg:            cfg,
		quit:           make(chan struct{}),
		entries:        make(chan entry),
		externalLabels: cfg.ExternalLabels.LabelSet,
	}

	err := cfg.Client.Validate()
	if err != nil {
		return nil, err
	}

	c.client, err = httpconfig.NewClientFromConfig(cfg.Client, "LokiGoClient", false, false)
	if err != nil {
		return nil, err
	}

	c.client.Timeout = cfg.Timeout

	c.wg.Add(1)
	go c.run()
	return c, nil
}

func (c *Client) run() {
	batches := map[string]*batch{}

	// Given the client handles multiple batches (1 per tenant) and each batch
	// can be created at a different point in time, we look for batches whose
	// max wait time has been reached every 10 times per BatchWait, so that the
	// maximum delay we have sending batches is 10% of the max waiting time.
	// We apply a cap of 10ms to the ticker, to avoid too frequent checks in
	// case the BatchWait is very low.
	minWaitCheckFrequency := 10 * time.Millisecond
	maxWaitCheckFrequency := c.cfg.BatchWait / 10
	if maxWaitCheckFrequency < minWaitCheckFrequency {
		maxWaitCheckFrequency = minWaitCheckFrequency
	}

	maxWaitCheck := time.NewTicker(maxWaitCheckFrequency)

	defer func() {
		// Send all pending batches
		for tenantID, batch := range batches {
			c.sendBatch(tenantID, batch)
		}

		c.wg.Done()
	}()

	for {
		select {
		case <-c.quit:
			return

		case e := <-c.entries:
			batch, ok := batches[e.tenantID]

			// If the batch doesn't exist yet, we create a new one with the entry
			if !ok {
				batches[e.tenantID] = newBatch(e)
				break
			}

			// If adding the entry to the batch will increase the size over the max
			// size allowed, we do send the current batch and then create a new one
			if batch.sizeBytesAfter(e) > c.cfg.BatchSize {
				c.sendBatch(e.tenantID, batch)

				batches[e.tenantID] = newBatch(e)
				break
			}

			// The max size of the batch isn't reached, so we can add the entry
			batch.add(e)

		case <-maxWaitCheck.C:
			// Send all batches whose max wait time has been reached
			for tenantID, batch := range batches {
				if batch.age() < c.cfg.BatchWait {
					continue
				}

				c.sendBatch(tenantID, batch)
				delete(batches, tenantID)
			}
		}
	}
}

func (c *Client) sendBatch(tenantID string, batch *batch) {
	var (
		err          error
		buf          []byte
		entriesCount int
	)

	buf, entriesCount, err = batch.encodeJSON()

	if err != nil {
		log.Println("msg", "error encoding batch", "error", err)
		return
	}

	ctx := context.Background()
	backoff := backoff.New(ctx, c.cfg.BackoffConfig)
	var status int
	for backoff.Ongoing() {
		status, err = c.send(ctx, tenantID, buf)

		// Only retry 429s, 500s and connection-level errors.
		if status > 0 && status != 429 && status/100 != 5 {
			break
		}

		log.Println("msg", "error sending batch, will retry", "status", status, "entriesCount", entriesCount, "error", err)

		backoff.Wait()
	}

	if err != nil {
		log.Println("msg", "final error sending batch", "status", status, "entriesCount", entriesCount, "error", err)
	}
}

func (c *Client) send(ctx context.Context, tenantID string, buf []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequest("POST", c.cfg.URL.String(), bytes.NewReader(buf))
	if err != nil {
		return -1, err
	}
	req = req.WithContext(ctx)

	req.Header.Set("Content-Type", JSONContentType)
	req.Header.Set("User-Agent", UserAgent)

	// If the tenant ID is not empty promtail is running in multi-tenant mode, so
	// we should send it to Loki
	if tenantID != "" {
		req.Header.Set("X-Scope-OrgID", tenantID)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return -1, err
	}
	defer helpers.LogError("closing response body", resp.Body.Close)

	if resp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(resp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s (%d): %s", resp.Status, resp.StatusCode, line)
	}
	return resp.StatusCode, err
}

func (c *Client) getTenantID(labels model.LabelSet) string {
	// Check if it has been overridden while processing the pipeline stages
	if value, ok := labels[ReservedLabelTenantID]; ok {
		return string(value)
	}

	// Check if has been specified in the config
	if c.cfg.TenantID != "" {
		return c.cfg.TenantID
	}

	// Defaults to an empty string, which means the X-Scope-OrgID header
	// will not be sent
	return ""
}

// Stop the client.
func (c *Client) Stop() {
	c.once.Do(func() { close(c.quit) })
	c.wg.Wait()
}

// Handle implement EntryHandler; adds a new line to the next batch; send is async.
func (c *Client) Handle(labels map[string]string, t time.Time, s string) error {
	ls := model.LabelSet{}
	for k, v := range labels {
		ls[model.LabelName(k)] = model.LabelValue(v)
	}

	if err := ls.Validate(); err != nil {
		return err
	}

	if len(c.externalLabels) > 0 {
		ls = c.externalLabels.Merge(ls)
	}

	// Get the tenant  ID in case it has been overridden while processing
	// the pipeline stages, then remove the special label
	tenantID := c.getTenantID(ls)
	if _, ok := ls[ReservedLabelTenantID]; ok {
		// Clone the label set to not manipulate the input one
		ls = ls.Clone()
		delete(ls, ReservedLabelTenantID)
	}

	c.entries <- entry{tenantID, ls, logproto.Value{
		fmt.Sprintf("%d", t.UnixNano()),
		s,
	}}
	return nil
}
