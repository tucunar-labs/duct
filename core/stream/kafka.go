package stream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Record is one Kafka message.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time

	raw *kgo.Record // what Commit hands back to the client
}

// Consumer is the part of a Kafka consumer-group client the layer needs. The
// franz-go implementation is NewKafka; tests use fakes.
type Consumer interface {
	// Poll blocks until at least one record is available (at most max are
	// returned) or ctx ends. A context error means "nothing yet".
	Poll(ctx context.Context, max int) ([]Record, error)
	// Commit marks recs, and everything before them in their partitions, as
	// processed. Offsets only move forward.
	Commit(ctx context.Context, recs []Record) error
	// OnAssigned registers f, called with the topic partitions this member
	// just received. Set it before the first Poll.
	OnAssigned(f func(topic string, partitions []int32))
	Close()
}

// KafkaConfig says how to reach the cluster. Secrets come from the caller
// (environment), never from duct.toml.
type KafkaConfig struct {
	Brokers   []string
	TLS       bool
	Mechanism string // "", PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512
	User      string
	Password  string
	ClientID  string
}

// ConsumerOptions select what one consumer group reads.
type ConsumerOptions struct {
	Group  string
	Topics []string
	// Start applies to a group with no committed offset: "earliest" (default),
	// "latest" or, if Since is set, the first message at or after Since.
	Start string
	Since time.Time
}

type kafkaConsumer struct {
	cl       *kgo.Client
	assigned atomic.Pointer[func(string, []int32)]
}

// NewKafka joins the consumer group with auto-commit off: offsets move only
// through Commit.
func NewKafka(cfg KafkaConfig, o ConsumerOptions) (Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: no brokers configured ([kafka] brokers in duct.toml)")
	}
	if o.Group == "" || len(o.Topics) == 0 {
		return nil, errors.New("kafka: group and topics are required")
	}
	k := &kafkaConsumer{}
	reset := kgo.NewOffset().AtStart()
	switch {
	case !o.Since.IsZero():
		reset = kgo.NewOffset().AfterMilli(o.Since.UnixMilli())
	case o.Start == "latest":
		reset = kgo.NewOffset().AtEnd()
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.Topics...),
		kgo.ConsumeResetOffset(reset),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500 * time.Millisecond),
		// After a crash (kill -9, power loss) the dead member's partitions move
		// on once its session expires: 10 s instead of the client's 45 s.
		kgo.SessionTimeout(10 * time.Second),
		kgo.RebalanceTimeout(20 * time.Second),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			if f := k.assigned.Load(); f != nil {
				for t, ps := range m {
					(*f)(t, ps)
				}
			}
		}),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	mech, err := saslMechanism(cfg)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	k.cl = cl
	return k, nil
}

func saslMechanism(cfg KafkaConfig) (sasl.Mechanism, error) {
	switch strings.ToUpper(cfg.Mechanism) {
	case "":
		return nil, nil
	case "PLAIN":
		return plain.Auth{User: cfg.User, Pass: cfg.Password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: cfg.User, Pass: cfg.Password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: cfg.User, Pass: cfg.Password}.AsSha512Mechanism(), nil
	}
	return nil, fmt.Errorf("kafka: sasl mechanism %q must be PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512", cfg.Mechanism)
}

func (k *kafkaConsumer) OnAssigned(f func(string, []int32)) { k.assigned.Store(&f) }

func (k *kafkaConsumer) Poll(ctx context.Context, max int) ([]Record, error) {
	k.cl.AllowRebalance() // the previous batch is done (or never started)
	f := k.cl.PollRecords(ctx, max)
	if f.IsClientClosed() {
		return nil, errors.New("kafka: client closed")
	}
	recs := make([]Record, 0, f.NumRecords())
	f.EachRecord(func(r *kgo.Record) {
		recs = append(recs, Record{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
			Key: r.Key, Value: r.Value, Timestamp: r.Timestamp, raw: r})
	})
	if len(recs) > 0 {
		return recs, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var errs []error
	for _, fe := range f.Errors() {
		errs = append(errs, fmt.Errorf("%s/%d: %w", fe.Topic, fe.Partition, fe.Err))
	}
	return nil, errors.Join(errs...)
}

func (k *kafkaConsumer) Commit(ctx context.Context, recs []Record) error {
	raw := make([]*kgo.Record, 0, len(recs))
	for _, r := range recs {
		if r.raw != nil {
			raw = append(raw, r.raw)
		}
	}
	if len(raw) == 0 {
		return nil
	}
	return k.cl.CommitRecords(ctx, raw...)
}

// Close leaves the group. CloseAllowingRebalance, not Close: with
// BlockRebalanceOnPoll a plain Close waits for a rebalance nobody allows.
func (k *kafkaConsumer) Close() { k.cl.CloseAllowingRebalance() }
