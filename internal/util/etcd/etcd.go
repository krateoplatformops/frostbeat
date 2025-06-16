package etcd

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func NewEtcdClient(endpoints []string) (cli *clientv3.Client, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = retryWithContext(ctx, 5, 2*time.Second, func() error {
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: 2 * time.Second,
		})
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("unable to create Etcd client after retries: %w", err)
	}

	return cli, nil
}

func retryWithContext(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		// Check if context was canceled or timed out
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err = fn(); err == nil {
			return nil
		}

		// Wait or return early if context is canceled during sleep
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("operation failed after %d attempts: %w", attempts, err)
}
