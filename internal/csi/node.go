// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package csi

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
)

// NodeServer implements the CSI Node service
type NodeServer struct {
	csi.UnimplementedNodeServer
	nodeID string
}

// NewNodeServer creates a new CSI node server
func NewNodeServer(nodeID string) *NodeServer {
	return &NodeServer{
		nodeID: nodeID,
	}
}

// NodeStageVolume mounts the HopsFS volume to a staging path
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path is required")
	}

	// Extract HopsFS connection parameters from volume context
	volumeContext := req.GetVolumeContext()
	hopsRpcAddress := volumeContext["hopsRpcAddress"]
	if hopsRpcAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "hopsRpcAddress is required in volume context")
	}

	srcDir := volumeContext["srcDir"]
	if srcDir == "" {
		srcDir = "/"
	}

	allowedPrefixes := strings.Split(volumeContext["allowedPrefixes"], ",")
	if len(allowedPrefixes) == 1 && allowedPrefixes[0] == "" {
		allowedPrefixes = []string{"*"}
	}

	// TLS configuration
	tlsConfig := hopsfsmount.TLSConfig{
		TLS:               volumeContext["tls"] == "true",
		RootCABundle:      volumeContext["rootCABundle"],
		ClientCertificate: volumeContext["clientCertificate"], 
		ClientKey:         volumeContext["clientKey"],
	}

	// Create mount directory if it doesn't exist
	if err := os.MkdirAll(req.StagingTargetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create staging directory: %v", err)
	}

	// Create HopsFS accessor
	retryPolicy := hopsfsmount.NewDefaultRetryPolicy(hopsfsmount.WallClock{})
	hdfsAccessor, err := hopsfsmount.NewHdfsAccessor(hopsRpcAddress, hopsfsmount.WallClock{}, tlsConfig)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create HopsFS accessor: %v", err)
	}

	ftHdfsAccessor := hopsfsmount.NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	// Create filesystem
	filesystem, err := hopsfsmount.NewFileSystem(
		[]hopsfsmount.HdfsAccessor{ftHdfsAccessor},
		srcDir,
		allowedPrefixes,
		false, // readOnly - could be configurable
		retryPolicy,
		hopsfsmount.WallClock{},
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create filesystem: %v", err)
	}

	// Mount the filesystem
	mountOptions := hopsfsmount.GetMountOptions(false) // readOnly
	_, err = filesystem.Mount(req.StagingTargetPath, mountOptions...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to mount filesystem: %v", err)
	}

	logger.Info(fmt.Sprintf("Successfully staged volume %s to %s", req.VolumeId, req.StagingTargetPath), nil)

	return &csi.NodeStageVolumeResponse{}, nil
}

// NodeUnstageVolume unmounts the HopsFS volume from the staging path
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path is required")
	}

	// Unmount the filesystem
	err := hopsfsmount.UnmountPath(req.StagingTargetPath)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to unmount %s: %v", req.StagingTargetPath, err), nil)
		return nil, status.Errorf(codes.Internal, "Failed to unmount: %v", err)
	}

	logger.Info(fmt.Sprintf("Successfully unstaged volume %s from %s", req.VolumeId, req.StagingTargetPath), nil)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

// NodePublishVolume bind mounts the staged volume to the target path
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}

	if req.StagingTargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Staging target path is required")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path is required")
	}

	// Create target directory if it doesn't exist
	if err := os.MkdirAll(req.TargetPath, 0755); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create target directory: %v", err)
	}

	// Bind mount from staging to target
	err := hopsfsmount.BindMount(req.StagingTargetPath, req.TargetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to bind mount: %v", err)
	}

	logger.Info(fmt.Sprintf("Successfully published volume %s to %s", req.VolumeId, req.TargetPath), nil)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmounts the volume from the target path
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}

	if req.TargetPath == "" {
		return nil, status.Error(codes.InvalidArgument, "Target path is required")
	}

	// Unmount the target path
	err := hopsfsmount.UnmountPath(req.TargetPath)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to unmount %s: %v", req.TargetPath, err), nil)
		return nil, status.Errorf(codes.Internal, "Failed to unmount: %v", err)
	}

	logger.Info(fmt.Sprintf("Successfully unpublished volume %s from %s", req.VolumeId, req.TargetPath), nil)

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetCapabilities returns the capabilities of the node
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			{
				Type: &csi.NodeServiceCapability_Rpc{
					Rpc: &csi.NodeServiceCapability_RPC{
						Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
					},
				},
			},
		},
	}, nil
}

// NodeGetInfo returns information about the node
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
	}, nil
}

// NodeGetVolumeStats returns volume statistics
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeGetVolumeStats is not implemented")
}

// NodeExpandVolume expands a volume
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "NodeExpandVolume is not implemented")
}