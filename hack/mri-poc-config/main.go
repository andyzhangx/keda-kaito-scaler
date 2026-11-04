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
// mri-poc-config produces OBSERVATION-ONLY JSON from local kubectl snapshots.
// It never connects to a cluster, writes an API or enables either attestation.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	mri "github.com/kaito-project/kaito/api/v1alpha1"
	kaito "github.com/kaito-project/kaito/api/v1beta1"
	core "k8s.io/api/core/v1"

	"github.com/kaito-project/keda-kaito-scaler/pkg/mripoc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: mri-poc-config SNAPSHOT_DIRECTORY")
	}
	read := func(name string, v any) error {
		b, e := os.ReadFile(filepath.Join(os.Args[1], name+".json"))
		if e != nil {
			return e
		}
		return json.Unmarshal(b, v)
	}
	p := new(mri.MultiRoleInference)
	if err := read("parent", p); err != nil {
		return err
	}
	c := mripoc.Config{Namespace: p.Namespace, Name: p.Name, UID: p.UID, ParentSpecSHA256: mripoc.ParentHash(p), Children: map[string]mripoc.Child{}}
	for _, r := range p.Spec.Roles {
		if r.Replicas != nil {
			return fmt.Errorf("parent replicas must be omitted in persisted API response")
		}
		role := string(r.Type)
		if role != "prefill" && role != "decode" {
			return fmt.Errorf("unsupported role %s", role)
		}
		is := new(kaito.InferenceSet)
		if err := read(role, is); err != nil {
			return err
		}
		cm := new(core.ConfigMap)
		if err := read(role+"-runtime", cm); err != nil {
			return err
		}
		if is.Namespace != p.Namespace || cm.Namespace != p.Namespace || cm.Name != r.RuntimeConfig || !mripoc.Owned(is, "kaito.sh", "MultiRoleInference", p.Name, p.UID) {
			return fmt.Errorf("snapshot identity mismatch for %s", role)
		}
		c.Children[role] = mripoc.Child{Name: is.Name, UID: is.UID, SpecSHA256: mripoc.ChildHash(is), RuntimeConfigSHA256: mripoc.Hash(cm.Data)}
	}
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	if _, err := mripoc.Parse(string(b)); err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
