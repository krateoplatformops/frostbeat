package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"context"

	"github.com/krateoplatformops/frostbeat/internal/manager"
	"github.com/krateoplatformops/frostbeat/internal/writers"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/kubernetes"
)

const (
	namespace     = "demo-system"
	labelSelector = "app=snowplow"
	batchSize     = 10
	batchPeriod   = 2 * time.Second
)

func main() {
	// podNames, err := getPodsByLabel(clientset, namespace, "app=snowplow")
	// if err != nil {
	// 	log.Fatalf("Errore nel recupero dei pod: %v", err)
	// }

	// for _, name := range podNames {
	// 	fmt.Println("Pod:", name)
	// }

	// Setup etcd
	/*
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{"localhost:2379"},
			DialTimeout: 5 * time.Second,
		})
		checkErr("etcd client", err)
		defer etcdCli.Close()
	*/

	config, err := getKubeConfig()
	checkErr("kube config", err)

	clientset, err := kubernetes.NewForConfig(config)
	checkErr("kube client", err)

	writer, err := writers.NewFileWriter("streamed-logs.txt")
	checkErr("log writer", err)

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager := manager.NewPodLogManager(manager.PodLogManagerOptions{
		Clientset:     clientset,
		Namespace:     namespace,
		LabelSelector: labelSelector,
		BatchPeriod:   batchPeriod,
		BatchSize:     batchSize,
	})

	err = manager.Start(ctx, &wg, writer)
	checkErr("start pod log manager", err)

	// Signal handling per graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Printf("Signal received: %v, shutting down...", sig)
	case <-ctx.Done():
		log.Println("Context done, shutting down...")
	}

	manager.Shutdown()

	wg.Wait()
	log.Println("Shutdown complete")
}

func getKubeConfig() (*rest.Config, error) {
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount"); err == nil {
		return rest.InClusterConfig()
	}
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		kubeconfig = os.ExpandEnv("$HOME/.kube/config")
	}
	return clientcmd.BuildConfigFromFlags("", kubeconfig)
}

func checkErr(msg string, err error) {
	if err != nil {
		log.Fatalf("[%s] %v", msg, err)
	}
}
