// Kafka adapter for the scheduler consumer. Builds a segmentio reader
// with mTLS (protocol SSL: CA + client cert/key loaded from files — the
// production 9093 listener) or optional SASL, WITHOUT touching the
// shared event-bus-data library.
package scheduling

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/config"
)

// KafkaConfig mirrors the frozen env surface (see T8 decision §6).
type KafkaConfig = config.SchedulerConfig

// NewDialer builds the transport dialer from the frozen config: mTLS
// files for SSL, SASL mechanism for SASL_*, plaintext only when
// explicitly chosen. File-loading errors name the file, never content.
func NewDialer(cfg KafkaConfig) (*kafka.Dialer, error) {
	d := &kafka.Dialer{Timeout: 10 * time.Second}
	switch cfg.KafkaSecurityProto {
	case "SSL", "SASL_SSL":
		tlsCfg := &tls.Config{}
		if cfg.KafkaTLSCAFile != "" {
			caPEM, err := os.ReadFile(cfg.KafkaTLSCAFile)
			if err != nil {
				return nil, fmt.Errorf("read TLS CA file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("TLS CA file %s contains no certificates", cfg.KafkaTLSCAFile)
			}
			tlsCfg.RootCAs = pool
		}
		if cfg.KafkaTLSCertFile != "" && cfg.KafkaTLSKeyFile != "" {
			cert, err := tls.LoadX509KeyPair(cfg.KafkaTLSCertFile, cfg.KafkaTLSKeyFile)
			if err != nil {
				return nil, fmt.Errorf("load TLS client keypair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		d.TLS = tlsCfg
	case "SASL_PLAINTEXT", "PLAINTEXT":
		// no TLS
	default:
		return nil, fmt.Errorf("unsupported protocol %q", cfg.KafkaSecurityProto)
	}
	if cfg.KafkaSecurityProto == "SASL_SSL" || cfg.KafkaSecurityProto == "SASL_PLAINTEXT" {
		m, err := saslMechanism(cfg.KafkaSASLMechanism, cfg.KafkaSASLUsername, cfg.KafkaSASLPassword)
		if err != nil {
			return nil, err
		}
		d.SASLMechanism = m
	}
	return d, nil
}

func saslMechanism(mechanism, username, password string) (sasl.Mechanism, error) {
	switch mechanism {
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, username, password)
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, username, password)
	case "PLAIN":
		return plain.Mechanism{Username: username, Password: password}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism %q", mechanism)
	}
}
