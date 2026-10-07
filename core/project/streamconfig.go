package project

// KafkaConfig is the [kafka] table of duct.toml: the cluster `duct stream` and
// `duct alerts` read from. Credentials never go here: DUCT_KAFKA_USER and
// DUCT_KAFKA_PASSWORD.
//
//	[kafka]
//	brokers   = ["127.0.0.1:9092"]
//	tls       = false
//	mechanism = "SCRAM-SHA-256"   # PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512; empty = no SASL
type KafkaConfig struct {
	Brokers   []string `toml:"brokers"`
	TLS       bool     `toml:"tls"`
	Mechanism string   `toml:"mechanism"`
}

// Environment variables with the Kafka SASL credentials.
const (
	EnvKafkaUser     = "DUCT_KAFKA_USER"
	EnvKafkaPassword = "DUCT_KAFKA_PASSWORD"
)

// StreamSettings is the [stream] table: its presence makes `duct serve` run
// the Kafka -> Bronze writers of streams/*.yaml in the same process.
type StreamSettings struct{}

// AlertsSettings is the [alerts] table: its presence makes `duct serve` run
// the alert rules of alerts/*.sql. Channels are named instances of an
// alert.channel module, referenced from the rules (CHANNELS ntfy:oncall):
//
//	[alerts]
//	group = "duct-alerts"            # Kafka consumer group (default duct-alerts)
//	instance = "alerts-1"            # name of this process in the shared state (default: random); several instances may run at once
//	lease_ttl = "15s"                # how long a claim/confirmation holds; a dead instance is replaced after about this long
//	[alerts.channels.oncall]
//	type = "ntfy"
//	topic = "planta-incendios"
//	token_env = "NTFY_TOKEN"         # <key>_env = name of an env var holding <key>
type AlertsSettings struct {
	Group    string                    `toml:"group"`
	Instance string                    `toml:"instance"`
	LeaseTTL string                    `toml:"lease_ttl"`
	Channels map[string]map[string]any `toml:"channels"`
}
