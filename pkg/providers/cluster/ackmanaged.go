/*
Copyright 2024 The CloudPilot AI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	ackclient "github.com/alibabacloud-go/cs-20151215/v5/client"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"

	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/apis/v1alpha1"
)

const (
	ackManagedClusterType = "ACKManaged"
	defaultNodeLabel      = "k8s.aliyun.com=true"
)

type ACKManaged struct {
	clusterID string
	region    string
	ackClient *ackclient.Client

	muClusterCNI sync.RWMutex
	clusterCNI   string
	cache        *cache.Cache
}

func NewACKManaged(clusterID string, region string, ackClient *ackclient.Client, cache *cache.Cache) *ACKManaged {
	return &ACKManaged{
		clusterID: clusterID,
		region:    region,
		ackClient: ackClient,
		cache:     cache,
	}
}

func (a *ACKManaged) LivenessProbe(_ *http.Request) error {
	return nil
}

func (a *ACKManaged) ClusterType() string {
	return ackManagedClusterType
}

func (a *ACKManaged) GetClusterCNI(_ context.Context) (string, error) {
	a.muClusterCNI.RLock()
	clusterCNI := a.clusterCNI
	a.muClusterCNI.RUnlock()

	if clusterCNI != "" {
		return clusterCNI, nil
	}

	response, err := a.ackClient.DescribeClusterDetail(tea.String(a.clusterID))
	if err != nil {
		return "", fmt.Errorf("failed to describe cluster: %w", err)
	}
	if response == nil || response.Body == nil || response.Body.MetaData == nil {
		return "", fmt.Errorf("empty cluster response")
	}
	// Parse metadata JSON string
	// clusterMetaData represents the metadata structure in cluster response
	type clusterMetaData struct {
		Capabilities struct {
			Network string `json:"Network"`
		} `json:"Capabilities"`
	}
	var metadata clusterMetaData
	if err := json.Unmarshal([]byte(*response.Body.MetaData), &metadata); err != nil {
		return "", fmt.Errorf("failed to unmarshal cluster metadata: %w", err)
	}

	a.muClusterCNI.Lock()
	a.clusterCNI = metadata.Capabilities.Network
	a.muClusterCNI.Unlock()

	return a.clusterCNI, nil
}

func (a *ACKManaged) GetSupportedImages(k8sVersion string) ([]Image, error) {
	// When query, the api ask to remove v from v1.6.0
	formatVersion := strings.TrimPrefix(k8sVersion, "v")
	req := &ackclient.DescribeKubernetesVersionMetadataRequest{
		Region:            tea.String(a.region),
		ClusterType:       tea.String("ManagedKubernetes"),
		KubernetesVersion: tea.String(formatVersion),
	}

	resp, err := a.ackClient.DescribeKubernetesVersionMetadata(req)
	if err != nil {
		return nil, fmt.Errorf("failed to describe k8s version metadata %w", err)
	}

	if resp == nil || resp.Body == nil || len(resp.Body) == 0 {
		return nil, fmt.Errorf("invalid response when describe k8s version metadata")
	} else if len(resp.Body[0].Images) == 0 {
		return nil, fmt.Errorf("no image found in k8s version metadata")
	}
	return lo.Map(resp.Body[0].Images, func(item *ackclient.DescribeKubernetesVersionMetadataResponseBodyImages, index int) Image {
		return Image{
			ImageID:      tea.StringValue(item.ImageId),
			ImageName:    tea.StringValue(item.ImageName),
			Platform:     tea.StringValue(item.Platform),
			OSVersion:    tea.StringValue(item.OsVersion),
			ImageType:    tea.StringValue(item.ImageType),
			OSType:       tea.StringValue(item.OsType),
			Architecture: tea.StringValue(item.Architecture),
		}
	}), nil
}

func (a *ACKManaged) UserData(ctx context.Context,
	labels map[string]string,
	taints []corev1.Taint,
	kubeletCfg *v1alpha1.KubeletConfiguration,
	userData *string,
	formatDataDisk bool) (string, error) {

	attach, err := a.getClusterAttachScripts(formatDataDisk, ctx)
	if err != nil {
		return "", err
	}
	ackScript := a.ackBootstrap(attach, labels, taints, kubeletCfg)
	cloudInit := NewCloudInit()

	if err := cloudInit.Merge(&ackScript); err != nil {
		return "", err
	}
	if err := cloudInit.Merge(userData); err != nil {
		return "", err
	}

	return cloudInit.Script()
}

func (a *ACKManaged) FeatureFlags() FeatureFlags {
	if cni, err := a.GetClusterCNI(context.TODO()); err == nil && cni == ClusterCNITypeFlannel {
		return FeatureFlags{
			PodsPerCoreEnabled:           false,
			SupportsENILimitedPodDensity: false,
		}
	}
	return FeatureFlags{
		PodsPerCoreEnabled:           true,
		SupportsENILimitedPodDensity: true,
	}
}

func (a *ACKManaged) getClusterAttachScripts(formatDataDisk bool, ctx context.Context) (string, error) {
	if cachedScript, ok := a.cache.Get(a.clusterID); ok {
		return cachedScript.(string), nil
	}

	reqPara := &ackclient.DescribeClusterAttachScriptsRequest{
		KeepInstanceName: tea.Bool(true),
		FormatDisk:       tea.Bool(formatDataDisk),
	}
	resp, err := a.ackClient.DescribeClusterAttachScripts(tea.String(a.clusterID), reqPara)
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to get node registration script")
		return "", err
	}
	respStr := tea.StringValue(resp.Body)
	if respStr == "" {
		err := errors.New("empty node registration script")
		log.FromContext(ctx).Error(err, "")
		return "", err
	}
	respStr = strings.ReplaceAll(respStr, "\r\n", "")

	runtime, runtimeVersion, err := a.getClusterAttachRuntimeConfiguration(ctx)
	if err != nil {
		// If error happen, we do nothing here
		log.FromContext(ctx).Error(err, "Failed to get cluster attach runtime configuration")
	} else {
		// Replace the runtime and runtime-version parameter
		respStr = regexp.MustCompile(`\s+--runtime\s+\S+\s+--runtime-version\s+\S+`).ReplaceAllString(
			respStr, fmt.Sprintf(" --runtime %s --runtime-version %s", runtime, runtimeVersion))
	}

	a.cache.SetDefault(a.clusterID, respStr)
	return respStr, nil
}

// We need to manually retrieve the runtime configuration of the nodepool, with the default node pool prioritized.
// If there is no default node pool, we will choose the runtime configuration of the node pool with the most HealthyNodes.
// In some cases, the DescribeClusterAttachScripts interface may return the Docker runtime in clusters running version 1.24,
// even though Docker runtime only supports clusters of version 1.22 and below.
//
//nolint:gocyclo
func (a *ACKManaged) getClusterAttachRuntimeConfiguration(ctx context.Context) (string, string, error) {
	resp, err := a.ackClient.DescribeClusterNodePools(tea.String(a.clusterID), &ackclient.DescribeClusterNodePoolsRequest{})
	if err != nil {
		log.FromContext(ctx).Error(err, "Failed to describe cluster nodepools")
		return "", "", err
	}
	if resp == nil || resp.Body == nil || resp.Body.Nodepools == nil {
		return "", "", fmt.Errorf("empty describe cluster nodepools response")
	}
	if len(resp.Body.Nodepools) == 0 {
		return "", "", fmt.Errorf("no nodepool found")
	}

	nodepools := resp.Body.Nodepools
	sort.Slice(nodepools, func(i, j int) bool {
		if nodepools[i].NodepoolInfo == nil || nodepools[j].NodepoolInfo == nil {
			return false
		}

		if nodepools[i].NodepoolInfo.IsDefault != nil && nodepools[j].NodepoolInfo.IsDefault != nil {
			if *nodepools[i].NodepoolInfo.IsDefault && !*nodepools[j].NodepoolInfo.IsDefault {
				return true
			}
			if !*nodepools[i].NodepoolInfo.IsDefault && *nodepools[j].NodepoolInfo.IsDefault {
				return false
			}
		}

		if nodepools[i].Status == nil || nodepools[j].Status == nil || nodepools[i].Status.HealthyNodes == nil || nodepools[j].Status.HealthyNodes == nil {
			return false
		}
		return *nodepools[i].Status.HealthyNodes > *nodepools[j].Status.HealthyNodes
	})

	targetNodepool := nodepools[0]
	if targetNodepool.KubernetesConfig == nil || targetNodepool.KubernetesConfig.Runtime == nil ||
		targetNodepool.KubernetesConfig.RuntimeVersion == nil {
		return "", "", fmt.Errorf("target describe cluster nodepool is empty")
	}
	return tea.StringValue(targetNodepool.KubernetesConfig.Runtime),
		tea.StringValue(targetNodepool.KubernetesConfig.RuntimeVersion), nil
}

func (a *ACKManaged) ackBootstrap(respStr string, labels map[string]string, taints []corev1.Taint,
	kubeletCfg *v1alpha1.KubeletConfiguration) string {

	var script bytes.Buffer
	// Add bash script header
	script.WriteString("#!/bin/bash\n\n")

	// Clean up the input string
	script.WriteString(respStr + " ")
	// Add labels
	script.WriteString(fmt.Sprintf("--labels %s ", a.formatLabels(labels)))
	// Add kubelet config
	cfg := convertNodeClassKubeletConfigToACKNodeConfig(kubeletCfg)
	script.WriteString(fmt.Sprintf("--node-config %s ", cfg))
	// Add taints
	script.WriteString(fmt.Sprintf("--taints %s\n\n", a.formatTaints(taints)))

	return script.String()
}

func (a *ACKManaged) formatLabels(labels map[string]string) string {
	labelsFormatted := fmt.Sprintf("%s,ack.aliyun.com=%s", defaultNodeLabel, a.clusterID)
	for key, value := range labels {
		labelsFormatted = fmt.Sprintf("%s,%s=%s", labelsFormatted, key, value)
	}
	return labelsFormatted
}

func (a *ACKManaged) formatTaints(taints []corev1.Taint) string {
	return strings.Join(lo.Map(taints, func(t corev1.Taint, _ int) string {
		return t.ToString()
	}), ",")
}

func (a *ACKManaged) DefaultOverhead(capacity corev1.ResourceList) cloudprovider.InstanceTypeOverhead {
	// referring to: https://help.aliyun.com/zh/ack/ack-managed-and-ack-dedicated/user-guide/resource-reservation-policy#0f5ffe176df7q
	// CPU overhead calculation
	cpuOverHead := calculateCPUOverhead(capacity.Cpu().MilliValue())

	// TODO: In a real environment, the formula does not produce accurate results,
	// consistently yielding values that are 200MiB larger than expected.
	// Memory overhead: min(11*pods + 255, memoryMi*0.25)
	memoryOverHead := int64(math.Min(float64(11*capacity.Pods().Value()+255), float64(capacity.Memory().Value()/1024*1024)*0.25)) + 200

	return cloudprovider.InstanceTypeOverhead{
		KubeReserved: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuOverHead/2, resource.DecimalSI),
			corev1.ResourceMemory: *resources.Quantity(fmt.Sprintf("%dMi", memoryOverHead/2)),
		},
		SystemReserved: corev1.ResourceList{
			corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuOverHead/2, resource.DecimalSI),
			corev1.ResourceMemory: *resources.Quantity(fmt.Sprintf("%dMi", memoryOverHead/2)),
		},
	}
}

// thresholds defines CPU overhead thresholds and their corresponding percentages
var thresholds = [...]struct {
	cores    int64
	overhead float64
}{
	{1000, 0.06},
	{2000, 0.01},
	{3000, 0.005},
	{4000, 0.005},
}

func calculateCPUOverhead(cpuM int64) int64 {
	var cpuOverHead int64

	// Calculate overhead for each threshold
	for _, t := range thresholds {
		if cpuM >= t.cores {
			cpuOverHead += int64(1000 * t.overhead)
		}
	}

	// Additional overhead for CPU > 4 cores (0.25%)
	if cpuM > 4000 {
		cpuOverHead += int64(float64(cpuM-4000) * 0.0025)
	}

	return cpuOverHead
}

type NodeConfig struct {
	KubeletConfig *ACKKubeletConfig `json:"kubelet_config,omitempty"`
}

type ACKKubeletConfig struct {
	MaxPods *int32 `json:"maxPods,omitempty"`
}

func convertNodeClassKubeletConfigToACKNodeConfig(kubeletCfg *v1alpha1.KubeletConfiguration) string {
	cfg := &NodeConfig{
		KubeletConfig: &ACKKubeletConfig{
			MaxPods: kubeletCfg.MaxPods,
		},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return base64.StdEncoding.EncodeToString([]byte("{}"))
	}
	return base64.StdEncoding.EncodeToString(data)
}
