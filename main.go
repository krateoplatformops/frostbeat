package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"

	"github.com/krateoplatformops/frostbeat/internal/manager"
	"github.com/krateoplatformops/frostbeat/internal/writers"
	"github.com/krateoplatformops/plumbing/env"
	"github.com/krateoplatformops/plumbing/slogs/pretty"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	"k8s.io/client-go/kubernetes"
)

const (
	serviceName = "frostbeat"
)

var (
	build string
)

func main() {
	kconfig := flag.String(clientcmd.RecommendedConfigPathFlag, "", "absolute path to the kubeconfig file")
	debugOn := flag.Bool("debug", env.Bool("DEBUG", false), "enable or disable debug logs")
	batchSize := flag.Int("batch-size", env.Int("BATCH_SIZE", 10), "Batch size")
	batchPeriod := flag.Duration("batch-period", env.Duration("BATCH_PERIOD", 3*time.Second), "Batch period")
	namespace := flag.String("namespace", env.String("NAMESPACE", "demo-system"), "Namespace")
	selector := flag.String("label-selector", env.String("SELECTOR", "app=snowplow"), "Pod label selector")

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Flags:")
		flag.PrintDefaults()
	}

	flag.Parse()

	logLevel := slog.LevelInfo
	if *debugOn {
		logLevel = slog.LevelDebug
	}

	log := slog.New(pretty.New(&slog.HandlerOptions{
		Level:     logLevel,
		AddSource: false,
	},
		pretty.WithDestinationWriter(os.Stderr),
		pretty.WithColor(),
		pretty.WithOutputEmptyAttrs(),
	)).With(slog.String("service", serviceName))

	if strings.TrimSpace(*namespace) == "" {
		log.Error("namespace cannot be empty")
		os.Exit(1)
	}

	if strings.TrimSpace(*selector) == "" {
		log.Error("pod label selector cannot be empty")
		os.Exit(1)
	}

	if *batchSize < 10 {
		batchSize = ptr.To(10)
	}

	if *batchPeriod <= 0 {
		batchPeriod = ptr.To(2 * time.Second)
	}

	var cfg *rest.Config
	var err error
	if len(*kconfig) > 0 {
		cfg, err = clientcmd.BuildConfigFromFlags("", *kconfig)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Error("unable to resolve rest.Config", slog.Any("err", err))
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Error("unable to create kubernetes.Clientset", slog.Any("err", err))
		os.Exit(1)
	}

	writer, err := writers.NewFileWriter("streamed-logs.txt")
	if err != nil {
		log.Error("unable to create stream writer", slog.Any("err", err))
		os.Exit(1)
	}

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := manager.NewPodLogManager(manager.PodLogManagerOptions{
		Clientset:     clientset,
		Namespace:     *namespace,
		LabelSelector: *selector,
		BatchPeriod:   *batchPeriod,
		BatchSize:     *batchSize,
	})

	err = manager.Start(ctx, &wg, writer)
	if err != nil {
		log.Error("unable to start PodLogManager", slog.Any("err", err))
		os.Exit(1)
	}

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Info("Signal received, shutting down...", slog.Any("signal", sig))
	case <-ctx.Done():
		log.Info("Context done, shutting down...")
	}

	manager.Shutdown()

	wg.Wait()
	log.Info("Shutdown complete")
}
