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
// Package mritest provides fake API graphs used only by cross-package lab tests.
package mritest

import (
	"context"
	"testing"

	mri "github.com/kaito-project/kaito/api/v1alpha1"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	keda "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	apps "k8s.io/api/apps/v1"
	autoscaling "k8s.io/api/autoscaling/v2"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kaito-project/keda-kaito-scaler/pkg/mripoc"
)

func Fixture(t *testing.T) (*mripoc.Guard, *kaito.InferenceSet, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{mri.AddToScheme, kaito.AddToScheme, keda.AddToScheme, core.AddToScheme, discovery.AddToScheme, apps.AddToScheme, autoscaling.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	p := &mri.MultiRoleInference{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "lab", UID: "parent"}, Spec: mri.MultiRoleInferenceSpec{Roles: []mri.MultiRoleInferenceRoleSpec{{Type: "prefill", RuntimeConfig: "prefill"}, {Type: "decode", RuntimeConfig: "decode"}}}}
	cfg := mripoc.Config{Namespace: p.Namespace, Name: p.Name, UID: p.UID, ParentSpecSHA256: mripoc.ParentHash(p), Activate: true, ExclusiveWritersVerified: true, ProxyMetricsVerified: true, Children: map[string]mripoc.Child{}}
	objects := []client.Object{p}
	var target *kaito.InferenceSet
	for _, role := range []string{"prefill", "decode"} {
		is := &kaito.InferenceSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-" + role, Namespace: "lab", UID: types.UID(role), Labels: map[string]string{mri.LabelMultiRoleInferenceParent: "demo", mri.LabelInferenceRole: role}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(p, mri.GroupVersion.WithKind("MultiRoleInference"))}}, Spec: kaito.InferenceSetSpec{Replicas: ptr.To(int32(1))}}
		cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: role, Namespace: "lab"}, Data: map[string]string{"config": "fixed"}}
		cfg.Children[role] = mripoc.Child{Name: is.Name, UID: is.UID, SpecSHA256: mripoc.ChildHash(is), RuntimeConfigSHA256: mripoc.Hash(cm.Data)}
		objects = append(objects, is, cm)
		if role == "prefill" {
			target = is
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(target), target); err != nil {
		t.Fatal(err)
	}
	return &mripoc.Guard{Config: cfg, Reader: c}, target, c
}
func AddMember(t *testing.T, g *mripoc.Guard, is *kaito.InferenceSet, c client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	is.Status.Replicas = int(*is.Spec.Replicas)
	is.Status.ReadyReplicas = int(*is.Spec.Replicas)
	// fake client does not register the custom status subresource here.
	if err := c.Update(ctx, is); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{mri.LabelMultiRoleInferenceParent: "demo", mri.LabelInferenceRole: is.Labels[mri.LabelInferenceRole], kaito.LabelWorkspaceName: name, "inferenceset.kaito.sh/created-by": is.Name, "statefulset.kubernetes.io/pod-name": name + "-0", "controller-revision-hash": "revision"}
	w := &kaito.Workspace{Status: kaito.WorkspaceStatus{TargetNodeCount: 1}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lab", UID: types.UID(name + "-workspace"), Labels: labels, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(is, kaito.GroupVersion.WithKind("InferenceSet"))}}}
	ss := &apps.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lab", UID: types.UID(name + "-stateful"), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(w, kaito.GroupVersion.WithKind("Workspace"))}}, Spec: apps.StatefulSetSpec{Replicas: ptr.To(int32(1))}, Status: apps.StatefulSetStatus{Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, CurrentRevision: "revision", UpdateRevision: "revision"}}
	pod := &core.Pod{Spec: core.PodSpec{NodeName: "gpu-node", Containers: []core.Container{{Name: "model", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceName("nvidia.com/gpu"): resource.MustParse("1")}}}}}, ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: "lab", UID: types.UID(name + "-pod"), Labels: labels, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ss, apps.SchemeGroupVersion.WithKind("StatefulSet"))}}, Status: core.PodStatus{Phase: core.PodRunning, PodIP: "10.1.1.1", Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}}
	svc := &core.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lab", UID: types.UID(name + "-service"), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(w, kaito.GroupVersion.WithKind("Workspace"))}}, Spec: core.ServiceSpec{Selector: map[string]string{kaito.LabelWorkspaceName: name, "statefulset.kubernetes.io/pod-name": name + "-0"}, Ports: []core.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(5000), Protocol: core.ProtocolTCP}}}}
	slice := &discovery.EndpointSlice{Ports: []discovery.EndpointPort{{Port: ptr.To(int32(5000)), Protocol: ptr.To(core.ProtocolTCP)}}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "lab", UID: "endpoints", Labels: map[string]string{discovery.LabelServiceName: name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(svc, core.SchemeGroupVersion.WithKind("Service"))}}, Endpoints: []discovery.Endpoint{{Addresses: []string{"10.1.1.1"}, Conditions: discovery.EndpointConditions{Ready: ptr.To(true)}, TargetRef: &core.ObjectReference{Kind: "Pod", Namespace: "lab", Name: name + "-0", UID: types.UID(name + "-pod")}}}}
	for _, obj := range []client.Object{w, ss, pod, svc, slice} {
		if err := c.Create(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}
	// Fake Create preserves supplied status; if that changes this positive test fails.
}

func HPA(so *keda.ScaledObject) *autoscaling.HorizontalPodAutoscaler {
	return &autoscaling.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "keda-hpa-" + so.Name, Namespace: so.Namespace, UID: types.UID("hpa-" + so.Name), OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(so, keda.GroupVersion.WithKind("ScaledObject"))}}, Spec: autoscaling.HorizontalPodAutoscalerSpec{MinReplicas: ptr.To(*so.Spec.MinReplicaCount), MaxReplicas: *so.Spec.MaxReplicaCount, Behavior: so.Spec.Advanced.HorizontalPodAutoscalerConfig.Behavior.DeepCopy(), ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: so.Spec.ScaleTargetRef.Name, Kind: so.Spec.ScaleTargetRef.Kind, APIVersion: so.Spec.ScaleTargetRef.APIVersion}, Metrics: []autoscaling.MetricSpec{{Type: autoscaling.ExternalMetricSourceType, External: &autoscaling.ExternalMetricSource{Metric: autoscaling.MetricIdentifier{Name: "composite-metric", Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"scaledobject.keda.sh/name": so.Name}}}, Target: autoscaling.MetricTarget{Type: autoscaling.ValueMetricType, Value: ptr.To(resource.MustParse("1"))}}}}}}
}
