// Copyright (c) KAITO authors.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package constants defines the InferenceSet autoscaling annotation keys and
// related identifiers shared across the scaler packages.
package scaledobject

import (
	"context"
	"strings"
	"testing"

	"github.com/expr-lang/expr"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	keda "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscaling "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kaito-project/keda-kaito-scaler/pkg/constants"
	"github.com/kaito-project/keda-kaito-scaler/pkg/mripoc"
)

func TestMRIBuildAndLivePolicy(t *testing.T) {
	for _, role := range []string{"prefill", "decode"} {
		t.Run(role, func(t *testing.T) {
			is := &kaito.InferenceSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-" + role, Namespace: "lab", UID: "child", Annotations: map[string]string{constants.AnnotationKeyMetrics: "- name: " + mripoc.ExpectedMetric(role) + "\n  type: gauge\n  source: modelpod\n  aggregation: service-avg\n  upthreshold: 10\n  downthreshold: 2\n"}}, Spec: kaito.InferenceSetSpec{Replicas: ptr.To(int32(1))}}
			b := Builder{ScalerNamespace: "lab", ScalerServiceName: "scaler", ScalerGRPCPort: 6000}
			so, err := b.BuildMRI(is, 1, 4, role)
			if err != nil {
				t.Fatal(err)
			}
			so.UID = "so"
			rules := so.Spec.Advanced.HorizontalPodAutoscalerConfig.Behavior
			if *rules.ScaleDown.SelectPolicy != autoscaling.DisabledPolicySelect || rules.ScaleUp.Policies[0].Value != 1 {
				t.Fatal("unsafe behavior")
			}
			for _, metric := range []float64{0, 11} {
				for _, gate := range []float64{0, 1} {
					env := map[string]any{"readiness_gate": gate, strings.ReplaceAll(mripoc.ExpectedMetric(role), ":", "_"): metric}
					program, err := expr.Compile(so.Spec.Advanced.ScalingModifiers.Formula, expr.Env(env))
					if err != nil {
						t.Fatal(err)
					}
					v, err := expr.Run(program, env)
					if err != nil {
						t.Fatal(err)
					}
					want := 1.0
					if gate == 1 && metric > 10 {
						want = 2
					}
					if v != want {
						t.Fatalf("formula %v want %v", v, want)
					}
				}
			}
			hp := &autoscaling.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "keda-hpa-" + is.Name, Namespace: is.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(so, keda.GroupVersion.WithKind("ScaledObject"))}}, Spec: autoscaling.HorizontalPodAutoscalerSpec{MinReplicas: ptr.To(int32(1)), MaxReplicas: 4, Behavior: rules.DeepCopy(), ScaleTargetRef: autoscaling.CrossVersionObjectReference{APIVersion: "kaito.sh/v1beta1", Kind: "InferenceSet", Name: is.Name}, Metrics: []autoscaling.MetricSpec{{Type: autoscaling.ExternalMetricSourceType, External: &autoscaling.ExternalMetricSource{Metric: autoscaling.MetricIdentifier{Name: "composite-metric", Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"scaledobject.keda.sh/name": so.Name}}}, Target: autoscaling.MetricTarget{Type: autoscaling.ValueMetricType, Value: ptr.To(resource.MustParse("1"))}}}}}}
			scheme := runtime.NewScheme()
			_ = autoscaling.AddToScheme(scheme)
			_ = keda.AddToScheme(scheme)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(hp, so).Build()
			g := &mripoc.Guard{Reader: c}
			if err := g.CheckServing(context.Background(), is, so, role); err != nil {
				t.Fatal(err)
			}
			for _, bad := range []string{"fallback", "idle", "restore", "pause", "selectPolicy", "max below live", "cached", "zero fallback", "HPA drift"} {
				t.Run(bad, func(t *testing.T) {
					broken := so.DeepCopy()
					switch bad {
					case "fallback":
						broken.Spec.Fallback = &keda.Fallback{}
					case "idle":
						broken.Spec.IdleReplicaCount = ptr.To(int32(0))
					case "restore":
						broken.Spec.Advanced.RestoreToOriginalReplicaCount = true
					case "pause":
						broken.Annotations["autoscaling.keda.sh/paused-replicas"] = "0"
					case "selectPolicy":
						broken.Spec.Advanced.HorizontalPodAutoscalerConfig.Behavior.ScaleDown.SelectPolicy = ptr.To(autoscaling.MaxChangePolicySelect)
					case "max below live":
						broken.Spec.MaxReplicaCount = ptr.To(int32(0))
					case "cached":
						broken.Spec.Triggers[0].UseCachedMetrics = true
					case "zero fallback":
						broken.Spec.Triggers[0].Metadata[constants.ZeroReplicaFallbackInMetadata] = "true"
					case "HPA drift":
						hp.Spec.Behavior.ScaleDown.SelectPolicy = ptr.To(autoscaling.MaxChangePolicySelect)
						if err := c.Update(context.Background(), hp); err != nil {
							t.Fatal(err)
						}
					}
					if err := g.CheckServing(context.Background(), is, broken, role); err == nil {
						t.Fatalf("accepted %s", bad)
					}
				})
			}
			for _, bounds := range [][2]int{{0, 4}, {1, 0}, {2, 4}, {1, 1 << 40}} {
				if _, err := b.BuildMRI(is, bounds[0], bounds[1], role); err == nil {
					t.Fatalf("accepted bounds %v", bounds)
				}
			}
		})
	}
}
