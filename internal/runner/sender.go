package runner

import (
	"context"
	"errors"
	"log"

	"shuttle/internal/config"
	"shuttle/internal/deliver"
)

// cloudflareSender is the live Sender: the §10.1 client behind the pipeline's
// seam, with one log line per send.
type cloudflareSender struct {
	client *deliver.Cloudflare
	logger *log.Logger
}

// NewSender returns the configured live sender. The token is read from the
// Keychain on each send, never here.
func NewSender(cfg *config.Config, logger *log.Logger) Sender {
	if logger == nil {
		logger = log.Default()
	}
	return &cloudflareSender{client: deliver.NewCloudflare(cfg), logger: logger}
}

func (s *cloudflareSender) Send(ctx context.Context, d Delivery) error {
	receipt, err := s.client.Send(ctx, deliver.Message{
		RunID: d.RunID, Agent: d.Agent, Subject: d.Subject, Text: d.Text, HTML: d.HTML,
	})
	s.logger.Printf("run=%s agent=%s delivery http=%d attempts=%d delivered=%d queued=%d bounced=%d error=%v",
		d.RunID, d.Agent, receipt.Status, receipt.Attempts, len(receipt.Delivered), len(receipt.Queued), len(receipt.Bounced), err)
	return err
}

// openDeliveryAuthIncident opens the §10.1 incident when the provider rejected
// the delivery credential. Any other delivery failure is recorded on the run
// alone.
func (p *Pipeline) openDeliveryAuthIncident(runID string, sendErr error) error {
	var cfErr *deliver.Error
	if !errors.As(sendErr, &cfErr) || !cfErr.Auth() {
		return nil
	}
	_, err := p.Ledger.DeliveryAuthIncident(runID, sendErr.Error())
	return err
}
