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
	"fmt"
	"testing"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/apis/v1alpha1"
)

func Test_convertNodeClassKubeletConfigToACKNodeConfig(t *testing.T) {
	kubeletCfg := &v1alpha1.KubeletConfiguration{
		MaxPods: tea.Int32(110),
	}
	d := convertNodeClassKubeletConfigToACKNodeConfig(kubeletCfg)
	assert.Equal(t, "eyJrdWJlbGV0X2NvbmZpZyI6eyJtYXhQb2RzIjoxMTB9fQ==", d)
}

// referring to: https://help.aliyun.com/zh/ack/ack-managed-and-ack-dedicated/user-guide/resource-reservation-policy#0f5ffe176df7q
func TestDefaultOverhead(t *testing.T) {
	provider := NewACKManaged("test-cluster", "cn-hangzhou", nil, nil)

	// ECS c7 / 1.28+
	cases := []struct {
		name         string
		cpuCores     int64
		memoryGi     int64
		maxPods      int64
		wantCPUMilli int64
		wantMemMi    int64
	}{
		{"2C4Gi-15pods", 2, 4, 15, 70, 420 + 200},
		{"4C8Gi-48pods", 4, 8, 48, 80, 982},
		{"8C16Gi-48pods", 8, 16, 48, 90, 982},
		{"16C32Gi-213pods", 16, 32, 213, 110, 2598 + 200},
		{"32C64Gi-213pods", 32, 64, 213, 150, 2598 + 200},
		{"64C128Gi-213pods", 64, 128, 213, 230, 2598 + 200},
		{"128C256Gi-423pods", 128, 256, 423, 390, 4908 + 200},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			capacity := corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewQuantity(tt.cpuCores, resource.DecimalSI),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%dGi", tt.memoryGi)),
				corev1.ResourcePods:   *resource.NewQuantity(tt.maxPods, resource.DecimalSI),
			}
			overhead := provider.DefaultOverhead(capacity)

			kubeCPU := overhead.KubeReserved.Cpu().MilliValue()
			sysCPU := overhead.SystemReserved.Cpu().MilliValue()
			assert.Equal(t, tt.wantCPUMilli, kubeCPU+sysCPU, "total CPU reserved")
			assert.Equal(t, kubeCPU, sysCPU, "CPU split 50/50")

			kubeMemMi := overhead.KubeReserved.Memory().Value() / (1024 * 1024)
			sysMemMi := overhead.SystemReserved.Memory().Value() / (1024 * 1024)
			assert.Equal(t, tt.wantMemMi, kubeMemMi+sysMemMi, "total memory reserved (Mi)")
			assert.Equal(t, kubeMemMi, sysMemMi, "memory split 50/50")
		})
	}
}
