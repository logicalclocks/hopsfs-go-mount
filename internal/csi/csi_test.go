// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.

package csi

import (
	"context"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount"
)

func TestIdentityServer(t *testing.T) {
	server := &IdentityServer{}

	// Test GetPluginInfo
	infoResp, err := server.GetPluginInfo(context.Background(), &csi.GetPluginInfoRequest{})
	assert.NoError(t, err)
	assert.Equal(t, "hopsfs.csi.hopsworks.ai", infoResp.Name)
	assert.Equal(t, hopsfsmount.VERSION, infoResp.VendorVersion)

	// Test GetPluginCapabilities
	capResp, err := server.GetPluginCapabilities(context.Background(), &csi.GetPluginCapabilitiesRequest{})
	assert.NoError(t, err)
	assert.Len(t, capResp.Capabilities, 1)
	assert.Equal(t, csi.PluginCapability_Service_CONTROLLER_SERVICE, 
		capResp.Capabilities[0].GetService().Type)

	// Test Probe
	probeResp, err := server.Probe(context.Background(), &csi.ProbeRequest{})
	assert.NoError(t, err)
	assert.True(t, probeResp.Ready.Value)
}

func TestNodeServer(t *testing.T) {
	server := NewNodeServer("test-node")

	// Test NodeGetInfo
	infoResp, err := server.NodeGetInfo(context.Background(), &csi.NodeGetInfoRequest{})
	assert.NoError(t, err)
	assert.Equal(t, "test-node", infoResp.NodeId)

	// Test NodeGetCapabilities
	capResp, err := server.NodeGetCapabilities(context.Background(), &csi.NodeGetCapabilitiesRequest{})
	assert.NoError(t, err)
	assert.Len(t, capResp.Capabilities, 1)
	assert.Equal(t, csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
		capResp.Capabilities[0].GetRpc().Type)
}

func TestControllerServer(t *testing.T) {
	server := &ControllerServer{}

	// Test ValidateVolumeCapabilities with valid capabilities
	req := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId: "test-volume",
		VolumeCapabilities: []*csi.VolumeCapability{
			{
				AccessType: &csi.VolumeCapability_Mount{
					Mount: &csi.VolumeCapability_MountVolume{},
				},
				AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
				},
			},
		},
	}
	
	resp, err := server.ValidateVolumeCapabilities(context.Background(), req)
	assert.NoError(t, err)
	assert.NotNil(t, resp.Confirmed)
	assert.Equal(t, req.VolumeCapabilities, resp.Confirmed.VolumeCapabilities)

	// Test ValidateVolumeCapabilities with unsupported block capabilities
	req.VolumeCapabilities[0].AccessType = &csi.VolumeCapability_Block{
		Block: &csi.VolumeCapability_BlockVolume{},
	}
	
	resp, err = server.ValidateVolumeCapabilities(context.Background(), req)
	assert.NoError(t, err)
	assert.Nil(t, resp.Confirmed)
	assert.Contains(t, resp.Message, "Block access type is not supported")
}