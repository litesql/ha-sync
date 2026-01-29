package extension

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/walterwanderley/sqlite"
)

type JetStreamSubscriberVirtualTable struct {
	virtualTableName string
	conn             *sqlite.Conn
	js               jetstream.JetStream
	timeout          time.Duration
	consumers        []consumer
	stmtMu           sync.Mutex
	mu               sync.Mutex
	rowIdentify      string
	logger           *slog.Logger
	loggerCloser     io.Closer
}

type consumer struct {
	cc jetstream.ConsumeContext

	subject string
	durable string
	policy  string
}

func NewJetStreamSubscriberVirtualTable(virtualTableName string, servers string, opts []nats.Option, timeout time.Duration, rowIdentify string, conn *sqlite.Conn, loggerDef string) (*JetStreamSubscriberVirtualTable, error) {

	logger, loggerCloser, err := loggerFromConfig(loggerDef)
	if err != nil {
		return nil, err
	}

	var js jetstream.JetStream
	if servers != "" {
		opts = append(opts,
			nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
				if err != nil {
					logger.Error("Got disconnected!", "reason", err)
				}
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				logger.Info("Got reconnected!", "url", nc.ConnectedUrl())
			}),
			nats.ClosedHandler(func(nc *nats.Conn) {
				if err := nc.LastError(); err != nil {
					logger.Error("Connection closed.", "reason", err)
				}
			}),
			nats.MaxReconnects(-1))
		nc, err := nats.Connect(servers, opts...)
		if err != nil {
			if loggerCloser != nil {
				loggerCloser.Close()
			}
			return nil, fmt.Errorf("failed to connect to nats: %w", err)
		}
		js, err = jetstream.New(nc)
		if err != nil {
			if loggerCloser != nil {
				loggerCloser.Close()
			}
			return nil, fmt.Errorf("failed to connect to jetstream: %w", err)
		}
	}

	return &JetStreamSubscriberVirtualTable{
		virtualTableName: virtualTableName,
		conn:             conn,
		js:               js,
		timeout:          timeout,
		rowIdentify:      rowIdentify,
		logger:           logger,
		loggerCloser:     loggerCloser,
		consumers:        make([]consumer, 0),
	}, nil
}

func (vt *JetStreamSubscriberVirtualTable) BestIndex(in *sqlite.IndexInfoInput) (*sqlite.IndexInfoOutput, error) {
	return &sqlite.IndexInfoOutput{EstimatedCost: 1000000}, nil
}

func (vt *JetStreamSubscriberVirtualTable) Open() (sqlite.VirtualCursor, error) {
	return newJetStreamSubscriptionsCursor(vt.consumers), nil
}

func (vt *JetStreamSubscriberVirtualTable) Disconnect() error {
	var err error
	if vt.loggerCloser != nil {
		err = vt.loggerCloser.Close()
	}
	for _, consumer := range vt.consumers {
		consumer.cc.Stop()
	}
	if vt.js != nil && !vt.js.Conn().IsClosed() {
		vt.js.Conn().Close()
	}

	return err
}

func (vt *JetStreamSubscriberVirtualTable) Destroy() error {
	return nil
}

func (vt *JetStreamSubscriberVirtualTable) Insert(values ...sqlite.Value) (int64, error) {
	if vt.js == nil {
		return 0, fmt.Errorf("not connected to jetstream")
	}
	subject := values[0].Text()
	if subject == "" {
		return 0, fmt.Errorf("subject is invalid")
	}
	var durable string
	if len(values) > 1 {
		durable = values[1].Text()
	}
	var policy string
	if len(values) > 2 {
		policy = values[2].Text()
	}
	vt.mu.Lock()
	defer vt.mu.Unlock()
	if vt.contains(subject) {
		return 0, fmt.Errorf("already subscribed to the %q subject", subject)
	}

	cfg := jetstream.ConsumerConfig{
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: subject,
		Durable:       durable,
		MaxAckPending: 1,
	}

	var loadSeqBydb bool
	if policy != "" {
		dp, err := getDeliverPolicy(policy)
		if err != nil {
			return 0, err
		}
		cfg.DeliverPolicy = dp.deliverPolicy
		cfg.OptStartSeq = dp.startSeq
		cfg.OptStartTime = dp.startTime
	} else {
		var seq uint64
		err := vt.conn.Exec("SELECT received_seq FROM ha_stats WHERE subject = ? LIMIT 1", func(stmt *sqlite.Stmt) error {
			seq = uint64(stmt.GetInt64("received_seq"))
			return nil
		}, subject)
		if err != nil {
			return 0, err
		}
		if seq > 0 {
			loadSeqBydb = true
			cfg.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			cfg.OptStartSeq = seq
		} else {
			cfg.DeliverPolicy = jetstream.DeliverAllPolicy
		}
	}

	var (
		ctx    = context.Background()
		cancel func()
	)
	if vt.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, vt.timeout)
		defer cancel()
	}
	stream := subject
	if i := strings.Index(subject, "."); i > 0 {
		stream = subject[0:i]
	}
	c, err := vt.js.CreateOrUpdateConsumer(ctx, stream, cfg)
	if err != nil {
		vt.logger.Debug("failed to create or update consumer: %v", err)
		if !loadSeqBydb {
			return 0, fmt.Errorf("failed to create or update consumer: %w", err)
		}
		var err2 error
		vt.logger.Debug("trying to get existing consumer")
		c, err2 = vt.js.Consumer(ctx, stream, cfg.Durable)
		if err2 != nil {
			return 0, errors.Join(err, fmt.Errorf("failed to get existing consumer: %w", err2))
		}
	}
	cc, err := c.Consume(vt.messageHandler)
	if err != nil {
		return 0, fmt.Errorf("failed to consume: %w", err)
	}
	vt.consumers = append(vt.consumers, consumer{
		cc:      cc,
		subject: subject,
		durable: durable,
		policy:  policy,
	})
	return 1, nil
}

func (vt *JetStreamSubscriberVirtualTable) Update(_ sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("UPDATE operations on %q are not supported", vt.virtualTableName)
}

func (vt *JetStreamSubscriberVirtualTable) Replace(old sqlite.Value, new sqlite.Value, _ ...sqlite.Value) error {
	return fmt.Errorf("REPLACE operations on %q are not supported", vt.virtualTableName)
}

func (vt *JetStreamSubscriberVirtualTable) Delete(v sqlite.Value) error {
	vt.mu.Lock()
	defer vt.mu.Unlock()
	index := v.Int()
	// slices are 0 based
	index--

	if index >= 0 && index < len(vt.consumers) {
		subscription := vt.consumers[index]
		subscription.cc.Stop()
		var (
			ctx    = context.Background()
			cancel func()
		)
		if vt.timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, vt.timeout)
			defer cancel()
		}
		err := vt.js.DeleteConsumer(ctx, subscription.subject, subscription.durable)
		if err != nil && !errors.Is(err, jetstream.ErrConsumerNotFound) {
			return err
		}
		vt.consumers = slices.Delete(vt.consumers, index, index+1)
	}
	return nil
}

func (vt *JetStreamSubscriberVirtualTable) contains(subject string) bool {
	for _, consumer := range vt.consumers {
		if consumer.subject == subject {
			return true
		}
	}
	return false
}

func (vt *JetStreamSubscriberVirtualTable) messageHandler(msg jetstream.Msg) {
	vt.stmtMu.Lock()
	defer vt.stmtMu.Unlock()
	meta, err := msg.Metadata()
	if err != nil {
		vt.logger.Error("failed to get message metadata", "error", err, "subject", msg.Subject())
		return
	}

	var cs ChangeSet
	cs.StreamSeq = meta.Sequence.Stream
	cs.Subject = msg.Subject()
	err = json.Unmarshal(msg.Data(), &cs)
	if err != nil {
		vt.logger.Error("failed to unmarshal CDC message", "error", err, "subject", msg.Subject(), "stream_seq", cs.StreamSeq)
		msg.Ack()
		return
	}

	if err := cs.Apply(vt.conn, vt.rowIdentify); err != nil {
		vt.logger.Error("failed to apply CDC message", "error", err, "subject", msg.Subject(), "stream_seq", cs.StreamSeq)
		return
	}

	if err := msg.Ack(); err != nil {
		vt.logger.Error("failed to ack CDC message", "error", err, "subject", msg.Subject(), "stream_seq", cs.StreamSeq)
		return
	}
}

type jetstreamSubscriptionsCursor struct {
	data    []consumer
	current consumer // current row that the cursor points to
	rowid   int64    // current rowid .. negative for EOF
}

func newJetStreamSubscriptionsCursor(data []consumer) *jetstreamSubscriptionsCursor {
	slices.SortFunc(data, func(a, b consumer) int {
		return cmp.Compare(a.subject, b.subject)
	})
	return &jetstreamSubscriptionsCursor{
		data: data,
	}
}

func (c *jetstreamSubscriptionsCursor) Next() error {
	// EOF
	if c.rowid < 0 || int(c.rowid) >= len(c.data) {
		c.rowid = -1
		return sqlite.SQLITE_OK
	}
	// slices are zero based
	c.current = c.data[c.rowid]
	c.rowid += 1

	return sqlite.SQLITE_OK
}

func (c *jetstreamSubscriptionsCursor) Column(ctx *sqlite.VirtualTableContext, i int) error {
	switch i {
	case 0:
		ctx.ResultText(c.current.subject)
	case 1:
		ctx.ResultText(c.current.durable)
	case 2:
		ctx.ResultText(c.current.policy)
	}

	return nil
}

func (c *jetstreamSubscriptionsCursor) Filter(int, string, ...sqlite.Value) error {
	c.rowid = 0
	return c.Next()
}

func (c *jetstreamSubscriptionsCursor) Rowid() (int64, error) {
	return c.rowid, nil
}

func (c *jetstreamSubscriptionsCursor) Eof() bool {
	return c.rowid < 0
}

func (c *jetstreamSubscriptionsCursor) Close() error {
	return nil
}

type deliverPolicyOpts struct {
	deliverPolicy jetstream.DeliverPolicy
	startSeq      uint64
	startTime     *time.Time
}

func getDeliverPolicy(policy string) (*deliverPolicyOpts, error) {
	var opts deliverPolicyOpts
	switch policy {
	case "all", "":
		opts.deliverPolicy = jetstream.DeliverAllPolicy
	case "last":
		opts.deliverPolicy = jetstream.DeliverLastPolicy
	case "new":
		opts.deliverPolicy = jetstream.DeliverNewPolicy
	default:
		matched, err := regexp.MatchString(`^by_start_sequence=\d+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			opts.deliverPolicy = jetstream.DeliverByStartSequencePolicy
			_, err := fmt.Sscanf(policy, "by_start_sequence=%d", &opts.startSeq)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start sequence: %w", err)
			}
			break

		}
		matched, err = regexp.MatchString(`^by_start_time=\w+`, policy)
		if err != nil {
			return nil, err
		}
		if matched {
			opts.deliverPolicy = jetstream.DeliverByStartTimePolicy
			dateTime := strings.TrimPrefix(policy, "by_start_time=")
			t, err := time.Parse(time.DateTime, dateTime)
			if err != nil {
				return nil, fmt.Errorf("invalid CDC subscriber start time: %w", err)
			}
			opts.startTime = &t
			break
		}
		return nil, fmt.Errorf("invalid deliver policy: %s", policy)
	}
	return &opts, nil
}
