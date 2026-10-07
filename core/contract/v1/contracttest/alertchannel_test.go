package contracttest_test

import (
	"context"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
)

// fakeChannel is a minimal conforming AlertChannel that records alerts.
type fakeChannel struct{ got []contract.Alert }

func (*fakeChannel) Name() string                                { return "fake" }
func (*fakeChannel) Init(context.Context, contract.Config) error { return nil }
func (*fakeChannel) Close() error                                { return nil }
func (f *fakeChannel) Notify(ctx context.Context, a contract.Alert) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.got = append(f.got, a)
	return nil
}

func TestFakeChannelConforms(t *testing.T) {
	contracttest.TestAlertChannel(t, func(*testing.T) (contract.AlertChannel, contract.Config) {
		return &fakeChannel{}, nil
	})
}
