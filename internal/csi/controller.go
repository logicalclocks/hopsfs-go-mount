// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ControllerServer implements the CSI Controller service for static provisioning
type ControllerServer struct {
	csi.UnimplementedControllerServer
}

// CreateVolume creates a new volume (not applicable for HopsFS static volumes)
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CreateVolume is not supported for static HopsFS volumes")
}

// DeleteVolume deletes a volume (not applicable for HopsFS static volumes)
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteVolume is not supported for static HopsFS volumes")
}

// ControllerPublishVolume attaches a volume to a node (not needed for filesystem)
func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerPublishVolume is not needed for HopsFS")
}

// ControllerUnpublishVolume detaches a volume from a node (not needed for filesystem)
func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerUnpublishVolume is not needed for HopsFS")
}

// ValidateVolumeCapabilities validates that the volume capabilities are supported
func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if req.VolumeId == "" {
		return nil, status.Error(codes.InvalidArgument, "Volume ID is required")
	}

	volumeCapabilities := req.GetVolumeCapabilities()
	if len(volumeCapabilities) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities are required")
	}

	// Check if all capabilities are supported
	for _, capability := range volumeCapabilities {
		accessMode := capability.GetAccessMode()
		if accessMode == nil {
			return nil, status.Error(codes.InvalidArgument, "Access mode is required")
		}

		// HopsFS supports ReadWriteMany and ReadOnlyMany
		switch accessMode.GetMode() {
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			 csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			 csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			 csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			 csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
			// All modes are supported
		default:
			return &csi.ValidateVolumeCapabilitiesResponse{
				Confirmed: nil,
				Message:   "Unsupported access mode",
			}, nil
		}

		// Check if it's a mount capability
		if mount := capability.GetMount(); mount != nil {
			// Mount capabilities are supported
		} else if block := capability.GetBlock(); block != nil {
			// Block capabilities are not supported
			return &csi.ValidateVolumeCapabilitiesResponse{
				Confirmed: nil,
				Message:   "Block access type is not supported",
			}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: volumeCapabilities,
		},
	}, nil
}

// ListVolumes lists available volumes (not applicable for static volumes)
func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListVolumes is not supported for static HopsFS volumes")
}

// GetCapacity returns available capacity (not implemented)
func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "GetCapacity is not implemented")
}

// ControllerGetCapabilities returns the capabilities of the controller
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: []*csi.ControllerServiceCapability{
			{
				Type: &csi.ControllerServiceCapability_Rpc{
					Rpc: &csi.ControllerServiceCapability_RPC{
						Type: csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
					},
				},
			},
		},
	}, nil
}

// CreateSnapshot creates a snapshot (not supported)
func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "CreateSnapshot is not supported")
}

// DeleteSnapshot deletes a snapshot (not supported)
func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "DeleteSnapshot is not supported")
}

// ListSnapshots lists snapshots (not supported)
func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ListSnapshots is not supported")
}

// ControllerExpandVolume expands a volume (not supported)
func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerExpandVolume is not supported")
}

// ControllerGetVolume gets volume information (not supported)
func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ControllerGetVolume is not supported")
}