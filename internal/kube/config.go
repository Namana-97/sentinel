package kube

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Config returns in-cluster configuration or falls back to KUBECONFIG for development.
func Config() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	path := os.Getenv("KUBECONFIG")
	if path == "" {
		userHome, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return nil, fmt.Errorf("resolve home: %w", homeErr)
		}
		path = filepath.Join(userHome, ".kube", "config")
	}
	cfg, err = clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes config: %w", err)
	}
	return cfg, nil
}
