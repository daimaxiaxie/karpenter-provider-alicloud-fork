/*
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

package tagging

import (
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/equality"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/apis/v1alpha1"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/operator/options"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/providers/instance"
	"github.com/cloudpilot-ai/karpenter-provider-alibabacloud/pkg/utils"
)

type Controller struct {
	kubeClient       client.Client
	instanceProvider instance.Provider
}

func NewController(kubeClient client.Client, instanceProvider instance.Provider) *Controller {
	return &Controller{
		kubeClient:       kubeClient,
		instanceProvider: instanceProvider,
	}
}

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *karpv1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "nodeclaim.tagging")

	stored := nodeClaim.DeepCopy()
	if !isTaggable(nodeClaim) {
		return reconcile.Result{}, nil
	}
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("provider-id", nodeClaim.Status.ProviderID))
	id, err := utils.ParseInstanceID(nodeClaim.Status.ProviderID)
	if err != nil {
		// We don't throw an error here since we don't want to retry until the ProviderID has been updated.
		log.FromContext(ctx).Error(err, "failed parsing instance id")
		return reconcile.Result{}, nil
	}
	if err = c.tagInstance(ctx, nodeClaim, id); err != nil {
		return reconcile.Result{}, cloudprovider.IgnoreNodeClaimNotFoundError(err)
	}
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
		v1alpha1.AnnotationInstanceTagged:                 "true",
		v1alpha1.AnnotationClusterNameTaggedCompatability: "true",
	})
	if !equality.Semantic.DeepEqual(nodeClaim, stored) {
		if err := c.kubeClient.Patch(ctx, nodeClaim, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.tagging").
		For(&karpv1.NodeClaim{}).
		WithEventFilter(predicate.NewPredicateFuncs(func(o client.Object) bool {
			return isTaggable(o.(*karpv1.NodeClaim))
		})).
		// Ok with using the default MaxConcurrentReconciles of 1 to avoid throttling from CreateTag write API
		WithOptions(controller.Options{
			RateLimiter: reasonable.RateLimiter(),
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

func (c *Controller) tagInstance(ctx context.Context, nc *karpv1.NodeClaim, id string) error {
	tags := map[string]string{
		v1alpha1.TagName:            nc.Status.NodeName,
		v1alpha1.TagNodeClaim:       nc.Name,
		v1alpha1.ECSClusterIDTagKey: options.FromContext(ctx).ClusterID,
	}

	// Remove tags which have been already populated
	instance, err := c.instanceProvider.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("tagging nodeclaim, %w", err)
	}
	tags = lo.OmitByKeys(tags, lo.Keys(instance.Tags))
	if len(tags) == 0 {
		return nil
	}

	// Ensures that no more than 1 CreateTags call is made per second. Rate limiting is required since CreateTags
	// shares a pool with other mutating calls (e.g. CreateFleet).
	defer time.Sleep(time.Second)
	if err := c.instanceProvider.CreateTags(ctx, id, tags); err != nil {
		return fmt.Errorf("tagging nodeclaim, %w", err)
	}
	return nil
}

func isTaggable(nc *karpv1.NodeClaim) bool {
	// Instance has already been tagged
	instanceTagged := nc.Annotations[v1alpha1.AnnotationInstanceTagged]
	clusterNameTagged := nc.Annotations[v1alpha1.AnnotationClusterNameTaggedCompatability]
	if instanceTagged == "true" && clusterNameTagged == "true" {
		return false
	}
	// Node name is not yet known
	if nc.Status.NodeName == "" {
		return false
	}
	// NodeClaim is currently terminating
	if !nc.DeletionTimestamp.IsZero() {
		return false
	}
	return true
}
