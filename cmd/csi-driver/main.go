// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	hopsfscsi "hopsworks.ai/hopsfsmount/internal/csi"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

var (
	endpoint = flag.String("endpoint", "unix:///var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai/csi.sock", "CSI endpoint")
	nodeID   = flag.String("node-id", "", "Node ID")
	version  = flag.Bool("version", false, "Print version")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("HopsFS CSI Driver\nVersion: %s\nGit Commit: %s\nBuild Time: %s\n", 
			hopsfsmount.VERSION, hopsfsmount.GITCOMMIT, hopsfsmount.BUILDTIME)
		os.Exit(0)
	}

	if *nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			logger.Fatal(fmt.Sprintf("Failed to get hostname: %v", err), nil)
		}
		*nodeID = hostname
	}

	// Initialize logger
	logger.Init()
	logger.Info("Starting HopsFS CSI Driver", logger.Fields{
		"version":   hopsfsmount.VERSION,
		"endpoint":  *endpoint,
		"node-id":   *nodeID,
	})

	// Create and start the CSI driver
	driver := &CSIDriver{
		endpoint: *endpoint,
		nodeID:   *nodeID,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		logger.Info("Received termination signal, shutting down gracefully", nil)
		cancel()
	}()

	if err := driver.Run(ctx); err != nil {
		logger.Fatal(fmt.Sprintf("Failed to run CSI driver: %v", err), nil)
	}
}

// CSIDriver represents the HopsFS CSI driver
type CSIDriver struct {
	endpoint string
	nodeID   string
	server   *grpc.Server
}

// Run starts the CSI driver gRPC server
func (d *CSIDriver) Run(ctx context.Context) error {
	// Parse the endpoint
	u, err := parseEndpoint(d.endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint: %v", err)
	}

	// Remove existing socket file if it exists
	if u.Scheme == "unix" {
		if err := os.Remove(u.Path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove existing socket: %v", err)
		}
	}

	// Listen on the endpoint
	listener, err := net.Listen(u.Scheme, u.Path)
	if err != nil {
		return fmt.Errorf("failed to listen on endpoint: %v", err)
	}
	defer listener.Close()

	// Create gRPC server
	d.server = grpc.NewServer()

	// Register CSI services
	identityServer := &hopsfscsi.IdentityServer{}
	nodeServer := hopsfscsi.NewNodeServer(d.nodeID)
	controllerServer := &hopsfscsi.ControllerServer{}

	csi.RegisterIdentityServer(d.server, identityServer)
	csi.RegisterNodeServer(d.server, nodeServer)
	csi.RegisterControllerServer(d.server, controllerServer)

	logger.Info(fmt.Sprintf("CSI driver listening on %s", d.endpoint), nil)

	// Start serving in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- d.server.Serve(listener)
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		logger.Info("Stopping CSI driver", nil)
		d.server.GracefulStop()
		return nil
	case err := <-errChan:
		return fmt.Errorf("gRPC server error: %v", err)
	}
}

// parseEndpoint parses a CSI endpoint string
func parseEndpoint(endpoint string) (*endpointURL, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty")
	}

	// Handle unix socket format
	if len(endpoint) >= 7 && endpoint[:7] == "unix://" {
		return &endpointURL{
			Scheme: "unix",
			Path:   endpoint[7:],
		}, nil
	}

	// Handle tcp format
	if len(endpoint) >= 6 && endpoint[:6] == "tcp://" {
		return &endpointURL{
			Scheme: "tcp",
			Path:   endpoint[6:],
		}, nil
	}

	// Default to unix socket
	return &endpointURL{
		Scheme: "unix",
		Path:   endpoint,
	}, nil
}

type endpointURL struct {
	Scheme string
	Path   string
}