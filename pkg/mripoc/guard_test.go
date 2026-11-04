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
package mripoc

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	mri "github.com/kaito-project/kaito/api/v1alpha1"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	keda "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	dto "github.com/prometheus/client_model/go"
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
)

func fixture(t *testing.T) (*Guard, *kaito.InferenceSet, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{mri.AddToScheme, kaito.AddToScheme, keda.AddToScheme, core.AddToScheme, discovery.AddToScheme, apps.AddToScheme, autoscaling.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	p := &mri.MultiRoleInference{ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "lab", UID: "parent"}, Spec: mri.MultiRoleInferenceSpec{Roles: []mri.MultiRoleInferenceRoleSpec{{Type: "prefill", RuntimeConfig: "prefill"}, {Type: "decode", RuntimeConfig: "decode"}}}}
	cfg := Config{Namespace: p.Namespace, Name: p.Name, UID: p.UID, ParentSpecSHA256: ParentHash(p), Activate: true, ExclusiveWritersVerified: true, ProxyMetricsVerified: true, Children: map[string]Child{}}
	objects := []client.Object{p}
	var target *kaito.InferenceSet
	for _, role := range []string{"prefill", "decode"} {
		is := &kaito.InferenceSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-" + role, Namespace: "lab", UID: types.UID(role), Labels: map[string]string{mri.LabelMultiRoleInferenceParent: "demo", mri.LabelInferenceRole: role}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(p, mri.GroupVersion.WithKind("MultiRoleInference"))}}, Spec: kaito.InferenceSetSpec{Replicas: ptr.To(int32(1))}}
		cm := &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: role, Namespace: "lab"}, Data: map[string]string{"config": "fixed"}}
		cfg.Children[role] = Child{Name: is.Name, UID: is.UID, SpecSHA256: ChildHash(is), RuntimeConfigSHA256: Hash(cm.Data)}
		objects = append(objects, is, cm)
		if role == "prefill" {
			target = is
		}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(target), target); err != nil {
		t.Fatal(err)
	}
	return &Guard{Config: cfg, Reader: c}, target, c
}
func TestAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Guard, *kaito.InferenceSet, client.Client)
	}{
		{"valid", func(*Guard, *kaito.InferenceSet, client.Client) {}},
		{"disabled", func(g *Guard, _ *kaito.InferenceSet, _ client.Client) { g.Config = Config{} }},
		{"observation", func(g *Guard, _ *kaito.InferenceSet, _ client.Client) { g.Config.Activate = false }},
		{"foreign UID", func(_ *Guard, is *kaito.InferenceSet, _ client.Client) { is.UID = "foreign" }},
		{"missing owner", func(_ *Guard, is *kaito.InferenceSet, c client.Client) {
			is.OwnerReferences = nil
			if err := c.Update(context.Background(), is); err != nil {
				t.Fatal(err)
			}
		}},
		{"wrong group", func(_ *Guard, is *kaito.InferenceSet, c client.Client) {
			is.OwnerReferences[0].APIVersion = "evil.io/v1"
			if err := c.Update(context.Background(), is); err != nil {
				t.Fatal(err)
			}
		}},
		{"fixed parent", func(g *Guard, _ *kaito.InferenceSet, c client.Client) {
			p := new(mri.MultiRoleInference)
			_ = c.Get(context.Background(), types.NamespacedName{Namespace: "lab", Name: "demo"}, p)
			p.Spec.Roles[0].Replicas = ptr.To(int32(1))
			if err := c.Update(context.Background(), p); err != nil {
				t.Fatal(err)
			}
		}},
		{"runtime changed", func(_ *Guard, _ *kaito.InferenceSet, c client.Client) {
			cm := new(core.ConfigMap)
			_ = c.Get(context.Background(), types.NamespacedName{Namespace: "lab", Name: "prefill"}, cm)
			cm.Data["config"] = "changed"
			if err := c.Update(context.Background(), cm); err != nil {
				t.Fatal(err)
			}
		}},
		{"other child missing", func(_ *Guard, _ *kaito.InferenceSet, c client.Client) {
			if err := c.Delete(context.Background(), &kaito.InferenceSet{ObjectMeta: metav1.ObjectMeta{Name: "demo-decode", Namespace: "lab"}}); err != nil {
				t.Fatal(err)
			}
		}},
		{"stale snapshot", func(_ *Guard, is *kaito.InferenceSet, _ client.Client) { is.ResourceVersion = "stale" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, is, c := fixture(t)
			tc.mutate(g, is, c)
			_, err := g.Authorize(context.Background(), is, true)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("unexpected authorization: %v", err)
			}
		})
	}
}
func TestParse(t *testing.T) {
	g, _, _ := fixture(t)
	b, _ := json.Marshal(g.Config)
	if _, err := Parse(string(b)); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{`{}`, `{"unknown":true}`, string(b) + `{}`, strings.Replace(string(b), `"exclusiveWritersVerified":true`, `"exclusiveWritersVerified":false`, 1)} {
		if _, err := Parse(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	if _, err := Parse(""); err != nil {
		t.Fatal(err)
	}
}
func TestGauge(t *testing.T) {
	now := time.Now()
	valid := func() *dto.MetricFamily {
		return &dto.MetricFamily{Type: ptr.To(dto.MetricType_GAUGE), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: ptr.To(3.0)}}}}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*dto.MetricFamily)
	}{
		{"valid", func(*dto.MetricFamily) {}}, {"negative", func(f *dto.MetricFamily) { f.Metric[0].Gauge.Value = ptr.To(-1.0) }}, {"nan", func(f *dto.MetricFamily) { f.Metric[0].Gauge.Value = ptr.To(math.NaN()) }}, {"infinity", func(f *dto.MetricFamily) { f.Metric[0].Gauge.Value = ptr.To(math.Inf(1)) }}, {"wrong type", func(f *dto.MetricFamily) { f.Type = ptr.To(dto.MetricType_COUNTER) }}, {"duplicate series", func(f *dto.MetricFamily) { f.Metric = append(f.Metric, f.Metric[0]) }}, {"missing", func(f *dto.MetricFamily) { f.Metric = nil }}, {"stale", func(f *dto.MetricFamily) { f.Metric[0].TimestampMs = ptr.To(now.Add(-31 * time.Second).UnixMilli()) }}, {"future", func(f *dto.MetricFamily) { f.Metric[0].TimestampMs = ptr.To(now.Add(time.Minute).UnixMilli()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := valid()
			tc.mutate(f)
			_, err := Gauge(f, now)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("unexpected gauge: %v", err)
			}
		})
	}
	if _, err := Gauge(nil, now); err == nil {
		t.Fatal("missing metric accepted")
	}
}
func addMember(t *testing.T, g *Guard, is *kaito.InferenceSet, c client.Client, name string) {
	t.Helper()
	ctx := context.Background()
	is.Status.Replicas = int(*is.Spec.Replicas)
	is.Status.ReadyReplicas = int(*is.Spec.Replicas)
	// fake client does not register the custom status subresource here.
	if err := c.Update(ctx, is); err != nil {
		t.Fatal(err)
	}
	labels := map[string]string{mri.LabelMultiRoleInferenceParent: "demo", mri.LabelInferenceRole: "prefill", kaito.LabelWorkspaceName: name, "inferenceset.kaito.sh/created-by": is.Name, "statefulset.kubernetes.io/pod-name": name + "-0", "controller-revision-hash": "revision"}
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
func TestMembership(t *testing.T) {
	for _, bad := range []string{"", "pending", "missing service", "wrong port", "rollout", "mixed pod"} {
		t.Run(bad, func(t *testing.T) {
			g, is, c := fixture(t)
			addMember(t, g, is, c, "ws")
			ctx := context.Background()
			switch bad {
			case "pending":
				is.Status.ReadyReplicas = 0
			case "missing service":
				_ = c.Delete(ctx, &core.Service{ObjectMeta: metav1.ObjectMeta{Name: "ws", Namespace: "lab"}})
			case "wrong port":
				svc := new(core.Service)
				_ = c.Get(ctx, types.NamespacedName{Namespace: "lab", Name: "ws"}, svc)
				svc.Spec.Ports[0].TargetPort = intstr.FromInt32(5001)
				_ = c.Update(ctx, svc)
			case "rollout":
				ss := new(apps.StatefulSet)
				_ = c.Get(ctx, types.NamespacedName{Namespace: "lab", Name: "ws"}, ss)
				ss.Status.UpdateRevision = "new"
				_ = c.Status().Update(ctx, ss)
			case "mixed pod":
				pod := &core.Pod{Spec: core.PodSpec{NodeName: "gpu-node", Containers: []core.Container{{Name: "model", Resources: core.ResourceRequirements{Limits: core.ResourceList{core.ResourceName("nvidia.com/gpu"): resource.MustParse("1")}}}}}, ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: "lab", Labels: map[string]string{kaito.LabelWorkspaceName: "ws"}}}
				_ = c.Create(ctx, pod)
			}
			endpoints, err := g.membership(ctx, is, "prefill")
			if (err == nil) != (bad == "") {
				t.Fatalf("unexpected membership %v %v", endpoints, err)
			}
		})
	}
}
func TestTargetersDoNotAdopt(t *testing.T) {
	g, is, c := fixture(t)
	ctx := context.Background()
	so := &keda.ScaledObject{ObjectMeta: metav1.ObjectMeta{Name: is.Name, Namespace: is.Namespace}, Spec: keda.ScaledObjectSpec{ScaleTargetRef: &keda.ScaleTarget{Name: is.Name, Kind: "InferenceSet", APIVersion: "kaito.sh/v1beta1"}}}
	if err := c.Create(ctx, so); err != nil {
		t.Fatal(err)
	}
	if _, err := g.CheckTargeters(ctx, is); err == nil {
		t.Fatal("foreign SO accepted")
	}
	if err := c.Delete(ctx, so); err != nil {
		t.Fatal(err)
	}
	h := &autoscaling.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "foreign", Namespace: is.Namespace}, Spec: autoscaling.HorizontalPodAutoscalerSpec{ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: is.Name, Kind: "InferenceSet", APIVersion: "kaito.sh/v1beta1"}}}
	if err := c.Create(ctx, h); err != nil {
		t.Fatal(err)
	}
	if _, err := g.CheckTargeters(ctx, is); err == nil {
		t.Fatal("foreign HPA accepted")
	}
}

func TestCollectionCompleteFreshMembership(t *testing.T) {
	for _, bad := range []string{"", "partial", "missing metric", "membership change", "authorization change", "timeout"} {
		t.Run(bad, func(t *testing.T) {
			g, is, c := fixture(t)
			is.Spec.Replicas = ptr.To(int32(2))
			addMember(t, g, is, c, "ws1")
			addMember(t, g, is, c, "ws2")
			ctx := context.Background()
			calls := 0
			value, err := g.collect(ctx, is, "prefill", ExpectedMetric("prefill"), func(ctx context.Context, url string) (map[string]*dto.MetricFamily, error) {
				calls++
				if calls == 2 {
					switch bad {
					case "partial":
						return nil, fmt.Errorf("scrape failed")
					case "missing metric":
						return nil, nil
					case "membership change":
						_ = c.Delete(ctx, &core.Service{ObjectMeta: metav1.ObjectMeta{Name: "ws1", Namespace: "lab"}})
					case "authorization change":
						g.Config.Activate = false
					case "timeout":
						return nil, context.DeadlineExceeded
					}
				}
				return map[string]*dto.MetricFamily{ExpectedMetric("prefill"): {Type: ptr.To(dto.MetricType_GAUGE), Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: ptr.To(float64(calls * 2))}}}}}, nil
			})
			if (err == nil) != (bad == "") {
				t.Fatalf("unexpected result %v %v", value, err)
			}
			if bad == "" && (value != 3 || calls != 2) {
				t.Fatalf("not a complete average: %v calls=%d", value, calls)
			}
		})
	}
}
