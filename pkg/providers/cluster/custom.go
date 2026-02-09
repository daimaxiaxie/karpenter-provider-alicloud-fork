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
	"context"
	"encoding/base64"
	"net/http"

	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/apis/v1alpha1"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
)

const customClusterType = "Custom"

type Custom struct {
}

func NewCustom() *Custom {
	return &Custom{}
}

func (c *Custom) UserData(ctx context.Context, labels map[string]string, taints []corev1.Taint, configuration *v1alpha1.KubeletConfiguration, userData *string, formatDataDisk bool) (string, error) {
	return base64.StdEncoding.EncodeToString([]byte(lo.FromPtr(userData))), nil
}

func (c *Custom) GetClusterCNI(ctx context.Context) (string, error) {
	return customClusterType, nil
}

func (c *Custom) LivenessProbe(request *http.Request) error {
	return nil
}

func (c *Custom) ClusterType() string {
	return customClusterType
}

func (c *Custom) GetSupportedImages(s string) ([]Image, error) {
	return nil, nil
}

func (c *Custom) FeatureFlags() FeatureFlags {
	return FeatureFlags{
		PodsPerCoreEnabled:           false,
		SupportsENILimitedPodDensity: false,
	}
}

func (c *Custom) DefaultOverhead(_ corev1.ResourceList) cloudprovider.InstanceTypeOverhead {
	return cloudprovider.InstanceTypeOverhead{}
}
