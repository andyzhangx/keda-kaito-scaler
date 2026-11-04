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
	"fmt"
	"math"
	"net/http"
	"reflect"
	"sort"
	"time"

	mri "github.com/kaito-project/kaito/api/v1alpha1"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	dto "github.com/prometheus/client_model/go"
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kaito-project/keda-kaito-scaler/pkg/util/promscrape"
)

const MaxCollectionAge = 30 * time.Second // two 15-second KEDA collection intervals; no cache

type endpoint struct{ URL, Epoch string }

// Membership deliberately supports only one GPU model pod / StatefulSet /
// Workspace. Multi-node Ray and rollouts hold rather than guessing coverage.
func (g *Guard) membership(ctx context.Context, is *kaito.InferenceSet, role string) ([]endpoint, error) {
	if is.Spec.Replicas == nil || *is.Spec.Replicas < 1 || is.Status.Replicas != int(*is.Spec.Replicas) || is.Status.ReadyReplicas != int(*is.Spec.Replicas) {
		return nil, fmt.Errorf("desired/actual/ready count mismatch")
	}
	workspaces := new(kaito.WorkspaceList)
	if err := g.Reader.List(ctx, workspaces, client.InNamespace(is.Namespace)); err != nil {
		return nil, err
	}
	var out []endpoint
	for i := range workspaces.Items {
		w := &workspaces.Items[i]
		owner := Owned(w, "kaito.sh", "InferenceSet", is.Name, is.UID)
		if !owner && w.Labels["inferenceset.kaito.sh/created-by"] != is.Name && !(w.Labels[mri.LabelMultiRoleInferenceParent] == g.Config.Name && w.Labels[mri.LabelInferenceRole] == role) {
			continue
		}
		if w.Status.TargetNodeCount != 1 {
			return nil, fmt.Errorf("requires fixed single-node GPU Workspace")
		}
		if !owner || w.UID == "" || !w.DeletionTimestamp.IsZero() || w.Labels[mri.LabelInferenceRole] != role || w.Labels[mri.LabelMultiRoleInferenceParent] != g.Config.Name {
			return nil, fmt.Errorf("mixed/terminating workspace membership")
		}
		ss := new(apps.StatefulSet)
		key := types.NamespacedName{Namespace: w.Namespace, Name: w.Name}
		if err := g.Reader.Get(ctx, key, ss); err != nil {
			return nil, err
		}
		if !Owned(ss, "kaito.sh", "Workspace", w.Name, w.UID) || !ss.DeletionTimestamp.IsZero() || ss.Spec.Replicas == nil || *ss.Spec.Replicas != 1 || ss.Status.ObservedGeneration != ss.Generation || ss.Status.Replicas != 1 || ss.Status.ReadyReplicas != 1 || ss.Status.UpdatedReplicas != 1 || ss.Status.CurrentRevision == "" || ss.Status.CurrentRevision != ss.Status.UpdateRevision {
			return nil, fmt.Errorf("workspace pending/rollout or unsupported multi-node topology")
		}
		pods := new(core.PodList)
		if err := g.Reader.List(ctx, pods, client.InNamespace(w.Namespace)); err != nil {
			return nil, err
		}
		var model *core.Pod
		for j := range pods.Items {
			pod := &pods.Items[j]
			owned := Owned(pod, "apps", "StatefulSet", ss.Name, ss.UID)
			if !owned && pod.Labels[kaito.LabelWorkspaceName] != w.Name {
				continue
			}
			if model != nil || !owned || pod.Name != w.Name+"-0" || pod.UID == "" || !pod.DeletionTimestamp.IsZero() || pod.Status.Phase != core.PodRunning || pod.Labels[mri.LabelInferenceRole] != role || pod.Labels[mri.LabelMultiRoleInferenceParent] != g.Config.Name || pod.Labels["controller-revision-hash"] != ss.Status.CurrentRevision {
				return nil, fmt.Errorf("mixed/terminating/rollout pod membership")
			}
			ready := false
			for _, c := range pod.Status.Conditions {
				if c.Type == core.PodReady && c.Status == core.ConditionTrue {
					ready = true
				}
			}
			gpu := false
			for _, container := range pod.Spec.Containers {
				if q, ok := container.Resources.Limits[core.ResourceName("nvidia.com/gpu")]; ok && q.Sign() > 0 {
					gpu = true
				}
			}
			if pod.Spec.NodeName == "" || !gpu {
				return nil, fmt.Errorf("requires scheduled GPU model pod")
			}
			if !ready {
				return nil, fmt.Errorf("model pod not ready")
			}
			model = pod
		}
		if model == nil {
			return nil, fmt.Errorf("missing model pod")
		}
		svc := new(core.Service)
		if err := g.Reader.Get(ctx, key, svc); err != nil {
			return nil, err
		}
		expected := map[string]string{kaito.LabelWorkspaceName: w.Name, "statefulset.kubernetes.io/pod-name": model.Name}
		if !Owned(svc, "kaito.sh", "Workspace", w.Name, w.UID) || !svc.DeletionTimestamp.IsZero() || !reflect.DeepEqual(svc.Spec.Selector, expected) || svc.Spec.ExternalName != "" {
			return nil, fmt.Errorf("service membership mismatch")
		}
		portOK := false
		for _, p := range svc.Spec.Ports {
			if p.Port == 80 && p.Protocol == core.ProtocolTCP && p.TargetPort == intstr.FromInt32(5000) {
				portOK = true
			}
		}
		if !portOK {
			return nil, fmt.Errorf("requires verified Service 80 -> pod 5000 (decode proxy, not Service 5001)")
		}
		slices := new(discovery.EndpointSliceList)
		if err := g.Reader.List(ctx, slices, client.InNamespace(w.Namespace), client.MatchingLabels{discovery.LabelServiceName: svc.Name}); err != nil {
			return nil, err
		}
		endpoints := 0
		sliceEpoch := ""
		for _, slice := range slices.Items {
			if len(slice.Ports) != 1 || slice.Ports[0].Port == nil || *slice.Ports[0].Port != 5000 || slice.Ports[0].Protocol == nil || *slice.Ports[0].Protocol != core.ProtocolTCP {
				return nil, fmt.Errorf("endpoint must use verified pod port 5000")
			}
			if !Owned(&slice, "", "Service", svc.Name, svc.UID) {
				return nil, fmt.Errorf("unowned service endpoint slice")
			}
			for _, e := range slice.Endpoints {
				endpoints++
				if e.TargetRef == nil || e.TargetRef.Kind != "Pod" || e.TargetRef.Namespace != model.Namespace || e.TargetRef.Name != model.Name || e.TargetRef.UID != model.UID || e.Conditions.Ready == nil || !*e.Conditions.Ready || (e.Conditions.Terminating != nil && *e.Conditions.Terminating) || len(e.Addresses) != 1 || e.Addresses[0] != model.Status.PodIP || model.Status.PodIP == "" {
					return nil, fmt.Errorf("endpoint coverage/readiness mismatch")
				}
			}
			sliceEpoch += string(slice.UID) + "/" + slice.ResourceVersion + ";"
		}
		if endpoints != 1 {
			return nil, fmt.Errorf("requires exactly one ready service endpoint")
		}
		// Status/resource-version changes invalidate in-flight samples conservatively.
		epoch := fmt.Sprintf("%s/%s/%s/%s/%s/%s/%s/%s", w.UID, w.ResourceVersion, ss.UID, ss.ResourceVersion, model.UID, model.ResourceVersion, svc.UID, svc.ResourceVersion)
		out = append(out, endpoint{URL: promscrape.DefaultURLBuilder("http", w.Name, w.Namespace, "80", "/metrics"), Epoch: epoch + sliceEpoch})
	}
	if len(out) != int(*is.Spec.Replicas) {
		return nil, fmt.Errorf("incomplete workspace coverage")
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out, nil
}

func Gauge(f *dto.MetricFamily, now time.Time) (float64, error) {
	if f == nil || f.GetType() != dto.MetricType_GAUGE || len(f.Metric) != 1 || f.Metric[0].Gauge == nil {
		return 0, fmt.Errorf("expected exactly one explicitly typed gauge series per model pod")
	}
	m := f.Metric[0]
	v := m.GetGauge().GetValue()
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, fmt.Errorf("nonfinite/negative gauge")
	}
	if m.TimestampMs != nil {
		age := now.Sub(time.UnixMilli(*m.TimestampMs))
		if age > MaxCollectionAge || age < -time.Second {
			return 0, fmt.Errorf("stale/future gauge")
		}
	}
	return v, nil
}

// Collect has no cache, retries or zero fallback. The same exact membership and
// authorization must still hold after all successful scrapes. One missing member
// invalidates the whole role, never an average of surviving endpoints.
func (g *Guard) Collect(ctx context.Context, is *kaito.InferenceSet, role, metric string) (float64, error) {
	hc := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("redirect forbidden") }}
	return g.collect(ctx, is, role, metric, func(ctx context.Context, url string) (map[string]*dto.MetricFamily, error) {
		return promscrape.FetchMetricFamilies(ctx, hc, url)
	})
}
func (g *Guard) collect(ctx context.Context, is *kaito.InferenceSet, role, metric string, fetch func(context.Context, string) (map[string]*dto.MetricFamily, error)) (float64, error) {
	ctx, cancel := context.WithTimeout(ctx, MaxCollectionAge)
	defer cancel()
	before, err := g.membership(ctx, is, role)
	if err != nil {
		return 0, err
	}
	if metric != "readiness_gate" && metric != ExpectedMetric(role) {
		return 0, fmt.Errorf("unexpected role metric")
	}
	sum := 0.0
	if metric != "readiness_gate" {
		for _, e := range before {
			families, err := fetch(ctx, e.URL)
			if err != nil {
				return 0, err
			}
			v, err := Gauge(families[metric], time.Now())
			if err != nil {
				return 0, err
			}
			sum += v
			if math.IsInf(sum, 0) {
				return 0, fmt.Errorf("gauge sum overflow")
			}
		}
	}
	live := new(kaito.InferenceSet)
	if err := g.Reader.Get(ctx, client.ObjectKeyFromObject(is), live); err != nil {
		return 0, err
	}
	if live.ResourceVersion != is.ResourceVersion {
		return 0, fmt.Errorf("child changed during collection")
	}
	if _, err := g.Authorize(ctx, live, true); err != nil {
		return 0, err
	}
	after, err := g.membership(ctx, live, role)
	if err != nil {
		return 0, err
	}
	if !reflect.DeepEqual(before, after) {
		return 0, fmt.Errorf("membership changed during collection")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if metric == "readiness_gate" {
		return 1, nil
	}
	return sum / float64(len(before)), nil
}

func (g *Guard) Ready(ctx context.Context, is *kaito.InferenceSet, role string) error {
	_, err := g.membership(ctx, is, role)
	return err
}
