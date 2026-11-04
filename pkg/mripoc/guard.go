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
// Package mripoc implements a deliberately narrow, opt-in disposable-lab contract.
// It is not a public MRI policy API. Every read uses the uncached API reader.
package mripoc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	mri "github.com/kaito-project/kaito/api/v1alpha1"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	keda "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	autoscaling "k8s.io/api/autoscaling/v2"
	core "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kaito-project/keda-kaito-scaler/pkg/constants"
)

const Env = "KAITO_MRI_POC"

// Config pins one MRI and its two children. Hashes freeze topology, GPU/node/TP
// settings and runtime ConfigMaps, not replica counts or child annotations.
// Activate is an operator attestation, not a substitute for admission/RBAC.
type Config struct {
	Namespace                string           `json:"namespace"`
	Name                     string           `json:"name"`
	UID                      types.UID        `json:"uid"`
	ParentSpecSHA256         string           `json:"parentSpecSHA256"`
	Children                 map[string]Child `json:"children"`
	Activate                 bool             `json:"activate"`
	ExclusiveWritersVerified bool             `json:"exclusiveWritersVerified"`
	ProxyMetricsVerified     bool             `json:"proxyMetricsVerified"`
}
type Child struct {
	RuntimeConfigSHA256 string    `json:"runtimeConfigSHA256"`
	Name                string    `json:"name"`
	UID                 types.UID `json:"uid"`
	SpecSHA256          string    `json:"specSHA256"`
}
type Guard struct {
	Config Config
	Reader client.Reader
}

func Parse(raw string) (Config, error) {
	var c Config
	if raw == "" {
		return c, nil
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("expected one JSON object")
	}
	if c.Namespace == "" || c.Name == "" || c.UID == "" || len(c.Children) != 2 {
		return c, fmt.Errorf("exactly one namespace/name/UID and two children required")
	}
	if !validHash(c.ParentSpecSHA256) {
		return c, fmt.Errorf("parent spec hash required")
	}
	for _, role := range []string{"prefill", "decode"} {
		child := c.Children[role]
		if child.Name != c.Name+"-"+role || child.UID == "" || (!validHash(child.SpecSHA256) || !validHash(child.RuntimeConfigSHA256)) {
			return c, fmt.Errorf("invalid %s identity/template hash", role)
		}
	}
	if c.Children["prefill"].UID == c.Children["decode"].UID {
		return c, fmt.Errorf("child UIDs must differ")
	}
	if c.Activate && (!c.ExclusiveWritersVerified || !c.ProxyMetricsVerified) {
		return c, fmt.Errorf("activation requires exclusive-writer and proxy/engine attestations")
	}
	return c, nil
}
func validHash(s string) bool { b, e := hex.DecodeString(s); return e == nil && len(b) == sha256.Size }
func Hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func ChildHash(is *kaito.InferenceSet) string {
	cp := is.DeepCopy()
	cp.Spec.Replicas = nil
	return Hash(cp.Spec)
}
func ParentHash(p *mri.MultiRoleInference) string {
	cp := p.DeepCopy()
	for i := range cp.Spec.Roles {
		cp.Spec.Roles[i].Replicas = nil
	}
	return Hash(cp.Spec)
}

// Candidate prevents the legacy path from becoming an authorization bypass,
// including malformed owner references, removed owners and replaced children.
func Candidate(is *kaito.InferenceSet) bool {
	for _, o := range is.OwnerReferences {
		if o.Kind == constants.MultiRoleInference {
			return true
		}
	}
	return is.Labels[mri.LabelMultiRoleInferenceParent] != "" || is.Labels[mri.LabelInferenceRole] != ""
}
func (g *Guard) Handles(is *kaito.InferenceSet) bool {
	if Candidate(is) {
		return true
	}
	if g == nil || is.Namespace != g.Config.Namespace {
		return false
	}
	for _, c := range g.Config.Children {
		if c.Name == is.Name || c.UID == is.UID {
			return true
		}
	}
	return false
}
func Owned(obj metav1.Object, group, kind, name string, uid types.UID) bool {
	o := metav1.GetControllerOf(obj)
	if o == nil {
		return false
	}
	gv, e := schema.ParseGroupVersion(o.APIVersion)
	return e == nil && gv.Group == group && o.Kind == kind && o.Name == name && o.UID == uid && uid != ""
}

// Authorize reads BOTH children and the stored parent on every call. A topology,
// UID, template or runtime-config change requires manual re-verification.
func (g *Guard) Authorize(ctx context.Context, is *kaito.InferenceSet, active bool) (string, error) {
	if g == nil || g.Reader == nil || g.Config.UID == "" {
		return "", fmt.Errorf("MRI POC disabled")
	}
	c := g.Config
	if active && (!c.Activate || !c.ExclusiveWritersVerified || !c.ProxyMetricsVerified) {
		return "", fmt.Errorf("MRI POC observation-only")
	}
	if is.Namespace != c.Namespace {
		return "", fmt.Errorf("namespace not authorized")
	}
	p := new(mri.MultiRoleInference)
	if err := g.Reader.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: c.Name}, p); err != nil {
		return "", err
	}
	if p.UID != c.UID || !p.DeletionTimestamp.IsZero() || len(p.Spec.Roles) != 2 || ParentHash(p) != c.ParentSpecSHA256 {
		return "", fmt.Errorf("parent identity/topology changed")
	}
	seen := map[string]bool{}
	found := ""
	for _, r := range p.Spec.Roles {
		role := string(r.Type)
		child, ok := c.Children[role]
		if !ok || seen[role] || r.Replicas != nil {
			return "", fmt.Errorf("requires exact prefill/decode with persisted nil replicas")
		}
		seen[role] = true
		live := new(kaito.InferenceSet)
		if err := g.Reader.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: child.Name}, live); err != nil {
			return "", err
		}
		if live.UID != child.UID || !live.DeletionTimestamp.IsZero() || !Owned(live, "kaito.sh", constants.MultiRoleInference, c.Name, c.UID) || live.Spec.Replicas == nil || *live.Spec.Replicas < 1 || ChildHash(live) != child.SpecSHA256 || live.Labels[mri.LabelInferenceRole] != role || live.Labels[mri.LabelMultiRoleInferenceParent] != c.Name {
			return "", fmt.Errorf("%s child identity/template/positive-count contract broken", role)
		}
		if r.RuntimeConfig == "" || !validHash(child.RuntimeConfigSHA256) {
			return "", fmt.Errorf("runtime config hash required")
		}
		cm := new(core.ConfigMap)
		if err := g.Reader.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: r.RuntimeConfig}, cm); err != nil {
			return "", err
		}
		if Hash(cm.Data) != child.RuntimeConfigSHA256 || len(cm.BinaryData) != 0 || !cm.DeletionTimestamp.IsZero() {
			return "", fmt.Errorf("runtime configuration changed")
		}
		if live.Name == is.Name && live.UID == is.UID {
			// Compare the caller snapshot too; stale cached objects must not authorize writes.
			if live.ResourceVersion != is.ResourceVersion {
				return "", fmt.Errorf("child changed during authorization")
			}
			found = role
		}
	}
	if found == "" {
		return "", fmt.Errorf("child not allowlisted")
	}
	return found, nil
}

func ExpectedMetric(role string) string {
	if role == "prefill" {
		return "vllm:num_requests_waiting"
	}
	return "vllm:num_requests_running"
}

// CheckTargeters rejects foreign or competing SO/HPA objects, never adopts them.
// This is a snapshot check: exclusive RBAC/admission is still mandatory.
func (g *Guard) CheckTargeters(ctx context.Context, is *kaito.InferenceSet) (*keda.ScaledObject, error) {
	sos := new(keda.ScaledObjectList)
	if err := g.Reader.List(ctx, sos, client.InNamespace(is.Namespace)); err != nil {
		return nil, err
	}
	var own *keda.ScaledObject
	for i := range sos.Items {
		so := &sos.Items[i]
		t := so.Spec.ScaleTargetRef
		targets := t != nil && t.Name == is.Name && t.Kind == constants.InferenceSet
		if so.Name != is.Name && !targets {
			continue
		}
		if so.Name != is.Name || !targets || t.APIVersion != constants.InferenceSetAPIVersion || !Owned(so, "kaito.sh", constants.InferenceSet, is.Name, is.UID) || so.Annotations[constants.AnnotationKeyManagedBy] != "keda-kaito-scaler" || !so.DeletionTimestamp.IsZero() {
			return nil, fmt.Errorf("foreign/competing ScaledObject %s", so.Name)
		}
		if err := SafeSO(so, is); err != nil {
			return nil, err
		}
		own = so
	}
	hpas := new(autoscaling.HorizontalPodAutoscalerList)
	if err := g.Reader.List(ctx, hpas, client.InNamespace(is.Namespace)); err != nil {
		return nil, err
	}
	count := 0
	for i := range hpas.Items {
		h := &hpas.Items[i]
		t := h.Spec.ScaleTargetRef
		targets := t.Name == is.Name && t.Kind == constants.InferenceSet
		if h.Name != "keda-hpa-"+is.Name && !targets {
			continue
		}
		if !targets {
			return nil, fmt.Errorf("same-name HPA has a foreign target")
		}
		count++
		if own == nil || !Owned(h, "keda.sh", "ScaledObject", own.Name, own.UID) || h.Name != "keda-hpa-"+own.Name || !h.DeletionTimestamp.IsZero() || t.APIVersion != constants.InferenceSetAPIVersion {
			return nil, fmt.Errorf("foreign/competing HPA %s", h.Name)
		}
	}
	if count > 1 {
		return nil, fmt.Errorf("multiple target HPAs")
	}
	return own, nil
}

// Unsafe options are rejected, not silently sanitized; the operator must stop
// writers before recovery. HPA behavior drift is separately repaired on owned objects.
func SafeSO(so *keda.ScaledObject, is *kaito.InferenceSet) error {
	s := so.Spec
	if s.MinReplicaCount == nil || *s.MinReplicaCount < 1 || s.MaxReplicaCount == nil || *s.MaxReplicaCount < *s.MinReplicaCount || is.Spec.Replicas == nil || *s.MaxReplicaCount < *is.Spec.Replicas || *s.MinReplicaCount > *is.Spec.Replicas || s.Fallback != nil || s.IdleReplicaCount != nil || s.Advanced == nil || s.Advanced.RestoreToOriginalReplicaCount {
		return fmt.Errorf("unsafe owned ScaledObject bounds/fallback/idle/restore")
	}
	for k := range so.Annotations {
		if k != constants.AnnotationKeyManagedBy {
			return fmt.Errorf("unsupported owned ScaledObject annotation %s (pause/transfer bypasses forbidden)", k)
		}
	}
	for _, t := range s.Triggers {
		if t.UseCachedMetrics || t.Metadata[constants.ZeroReplicaFallbackInMetadata] != "" {
			return fmt.Errorf("cached/zero fallback trigger forbidden")
		}
	}
	return nil
}

// Serving checks the live SO intent and actual owned HPA, not only identity.
// Any drift freezes metrics until repaired by reconciliation/operator recovery.
func (g *Guard) CheckServing(ctx context.Context, is *kaito.InferenceSet, so *keda.ScaledObject, role string) error {
	if err := SafeSO(so, is); err != nil {
		return err
	}
	a := so.Spec.Advanced
	if a.HorizontalPodAutoscalerConfig == nil || a.HorizontalPodAutoscalerConfig.Behavior == nil {
		return fmt.Errorf("missing HPA behavior")
	}
	b := a.HorizontalPodAutoscalerConfig.Behavior
	if b.ScaleDown == nil || b.ScaleDown.SelectPolicy == nil || *b.ScaleDown.SelectPolicy != autoscaling.DisabledPolicySelect || b.ScaleUp == nil || len(b.ScaleUp.Policies) != 1 || b.ScaleUp.Policies[0].Type != autoscaling.PodsScalingPolicy || b.ScaleUp.Policies[0].Value != 1 || b.ScaleUp.Policies[0].PeriodSeconds < 60 {
		return fmt.Errorf("requires enforced scale-up-only one-at-a-time behavior")
	}
	name := regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(ExpectedMetric(role), "_")
	pattern := `^\(readiness_gate == 1 && ` + name + ` > [0-9.eE+\-]+\) \? 2\.0 : 1\.0$`
	if !regexp.MustCompile(pattern).MatchString(a.ScalingModifiers.Formula) || a.ScalingModifiers.Target != "1" || a.ScalingModifiers.MetricType != autoscaling.ValueMetricType || a.ScalingModifiers.ActivationTarget != "" || len(so.Spec.Triggers) != 2 || so.Spec.PollingInterval == nil || *so.Spec.PollingInterval != 15 {
		return fmt.Errorf("SO policy drift")
	}
	hp := new(autoscaling.HorizontalPodAutoscaler)
	if err := g.Reader.Get(ctx, types.NamespacedName{Namespace: is.Namespace, Name: "keda-hpa-" + so.Name}, hp); err != nil {
		return err
	}
	if !hp.DeletionTimestamp.IsZero() || hp.Spec.ScaleTargetRef.Name != is.Name || hp.Spec.ScaleTargetRef.Kind != constants.InferenceSet || hp.Spec.ScaleTargetRef.APIVersion != constants.InferenceSetAPIVersion || !Owned(hp, "keda.sh", "ScaledObject", so.Name, so.UID) || !apiequality.Semantic.DeepEqual(hp.Spec.Behavior, b) || hp.Spec.MinReplicas == nil || *hp.Spec.MinReplicas != *so.Spec.MinReplicaCount || hp.Spec.MaxReplicas != *so.Spec.MaxReplicaCount || hp.Spec.MaxReplicas < *is.Spec.Replicas {
		return fmt.Errorf("live HPA drift or stale bounds")
	}
	if len(hp.Spec.Metrics) != 1 || hp.Spec.Metrics[0].Type != autoscaling.ExternalMetricSourceType || hp.Spec.Metrics[0].External == nil {
		return fmt.Errorf("live HPA metric drift")
	}
	metric := hp.Spec.Metrics[0].External
	if metric.Metric.Name != "composite-metric" || metric.Target.Type != autoscaling.ValueMetricType || metric.Target.Value == nil || metric.Target.Value.AsApproximateFloat64() != 1 || metric.Metric.Selector == nil || len(metric.Metric.Selector.MatchExpressions) != 0 || !reflect.DeepEqual(metric.Metric.Selector.MatchLabels, map[string]string{"scaledobject.keda.sh/name": so.Name}) {
		return fmt.Errorf("live HPA composite target drift")
	}
	return nil
}
