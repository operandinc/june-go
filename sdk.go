// June is a client library for the June analytics API.
package june

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// Logf is a function that logs a message.
type Logf func(format string, args ...any)

func defaultLogger() Logf {
	logger := log.New(os.Stdout, "june", log.LstdFlags|log.Lshortfile)
	return func(format string, args ...any) {
		logger.Printf(format, args...)
	}
}

// AllowFunc is a function that returns true if the event should be allowed to be uploaded.
// This is useful for testing, or if you want to filter out certain events (i.e. in development).
type AllowFunc func(msg Message) bool

// Client is a client for the June analytics API.
// It is safe for concurrent use by multiple goroutines, and events
// will be queued to be uploaded via a pool of background workers.
type Client struct {
	writeKey     string
	httpClient   *http.Client
	logf         Logf
	bufferSize   int
	numWorkers   int
	allowFuncs   []AllowFunc
	messageQueue chan Message
	waitGroup    sync.WaitGroup
	shutdownFunc context.CancelFunc
}

// ClientOpt is a function that configures a Client.
type ClientOpt func(*Client)

// WithHTTPClient configures the HTTP client used by the client.
func WithHTTPClient(client *http.Client) ClientOpt {
	return func(c *Client) {
		c.httpClient = client
	}
}

// WithBufferSize configures the size of the message queue.
// By default, we use a queue size of 64 which should be large enough
// for most use cases. If you are sending a large number of events,
// you may want to increase this.
func WithBufferSize(size int) ClientOpt {
	return func(c *Client) {
		c.bufferSize = size
	}
}

// WithNumWorkers configures the number of workers that will upload
// events to the API. By default, we use 3 workers which should be
// sufficient for most use cases. If you are sending a large number
// of events, you may want to increase this.
func WithNumWorkers(num int) ClientOpt {
	return func(c *Client) {
		c.numWorkers = num
	}
}

// WithLogf configures the function used to log messages. This is used
// to log any errors that occur while uploading events to the API.
// By default, we write messages to stdout using the `log` package.
func WithLogf(logf Logf) ClientOpt {
	return func(c *Client) {
		c.logf = logf
	}
}

// WithAllowFunc configures a function that returns true if the event should be allowed to be uploaded.
// For instance, in development environments, you may want to block events from being uploaded.
func WithAllowFunc(allowFunc AllowFunc) ClientOpt {
	return func(c *Client) {
		c.allowFuncs = append(c.allowFuncs, allowFunc)
	}
}

// NewClient creates a new client for the June analytics API.
func NewClient(writeKey string, opts ...ClientOpt) *Client {
	workerCtx, cancel := context.WithCancel(context.Background())
	c := &Client{
		writeKey:     writeKey,
		httpClient:   http.DefaultClient,
		logf:         defaultLogger(),
		bufferSize:   64,
		numWorkers:   3,
		shutdownFunc: cancel,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.messageQueue = make(chan Message, c.bufferSize)
	for i := 0; i < c.numWorkers; i++ {
		c.waitGroup.Add(1)
		go func() {
			c.worker(workerCtx)
			c.waitGroup.Done()
		}()
	}
	return c
}

// Enqueue adds a message to the queue to be uploaded to the API.
// If the queue is full, this function will block until there is
// space in the queue. The client should (ideally) be configured
// with a big enough message buffer to avoid this, and enough workers
// to upload events quickly enough to avoid blocking.
func (c *Client) Enqueue(msg Message) {
	if c == nil {
		return // No-op for nil client.
	}
	for _, allowFunc := range c.allowFuncs {
		if !allowFunc(msg) {
			return
		}
	}
	msg.SetTimestamp(time.Now()) // Record the proper timestamp.
	c.messageQueue <- msg
}

// Close shuts down the client, and waits for all pending events to be uploaded.
// It's important to call this function before terminating your program, otherwise,
// some events may not be uploaded correctly.
func (c *Client) Close() {
	if c == nil {
		return // No-op for nil client.
	}
	c.shutdownFunc()
	c.waitGroup.Wait()
	for len(c.messageQueue) > 0 {
		msg := <-c.messageQueue
		if err := c.upload(context.Background(), msg); err != nil {
			c.logf("error uploading %q analytics message: %v", msg.Kind(), err)
		}
	}
	close(c.messageQueue)
}

func (c *Client) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-c.messageQueue:
			if err := c.upload(context.Background(), msg); err != nil {
				c.logf("error uploading %q analytics message: %v", msg.Kind(), err)
				continue
			}
			c.logf("successfully uploaded %q analytics message", msg.Kind())
		}
	}
}

func (c *Client) upload(ctx context.Context, message Message) error {
	body, err := json.Marshal(message)
	if err != nil {
		return err
	}

	url := "https://api.june.so/sdk/" + message.Kind()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+c.writeKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("june: unexpected status code: %d (%s)", resp.StatusCode, resp.Status)
	}

	return nil
}

// Message is an interface implemented by all message types.
type Message interface {
	Kind() string // The event needs to be sent to https://api.june.so/api/$kind.
	SetTimestamp(time.Time)
}

var (
	_ Message = (*Identify)(nil)
	_ Message = (*Group)(nil)
	_ Message = (*Track)(nil)
)

// Identify is a message that identifies a user.
type Identify struct {
	UserID    string         `json:"userId"`
	Traits    map[string]any `json:"traits"`
	Timestamp time.Time      `json:"timestamp"`
}

func (i *Identify) Kind() string {
	return "identify"
}

func (i *Identify) SetTimestamp(ts time.Time) {
	i.Timestamp = ts
}

// Group is a message that identifies a group, and associates it with a user.
type Group struct {
	GroupID   string         `json:"groupId"`
	UserID    string         `json:"userId"`
	Traits    map[string]any `json:"traits"`
	Timestamp time.Time      `json:"timestamp"`
}

func (g *Group) Kind() string {
	return "group"
}

func (g *Group) SetTimestamp(ts time.Time) {
	g.Timestamp = ts
}

// Track is a message that tracks an event.
type Track struct {
	UserID     string         `json:"userId"`
	Event      string         `json:"event"`
	Properties map[string]any `json:"properties,omitempty"`
	Context    map[string]any `json:"context,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
}

func (t *Track) Kind() string {
	return "track"
}

func (t *Track) SetTimestamp(ts time.Time) {
	t.Timestamp = ts
}
