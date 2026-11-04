# Disposable-lab MRI prefill/decode role autoscaling POC

**Not production support. Disabled by default. No cluster deployment or live
inference/scale test is claimed by this change.** This is executable scaler-side
preparation for section 0 of the
[implementation plan](https://github.com/andyzhangx/demo/blob/d2828050ea9657eda90aa9157cbdb949d9b552e8/llm/kaito/mri-pd-autoscaling-poc-implementation-plan.md).
Use the latest section-0 acceptance requirements if the plan changes.

## Boundary and prerequisites

- Exactly one namespace/name/UID-pinned MRI, exactly two roles `prefill` and
  `decode`, and the corresponding name/UID-pinned MRI-owned InferenceSets.
- KAITO prerequisite: build/install the actual controller/webhook and CRD changes
  at [andyzhangx/kaito commit e1902f7](https://github.com/andyzhangx/kaito/commit/e1902f7e81e470d65686bb5b5e6b09732c7b1755),
  on `andyzhangx/mri-pd-autoscaling-poc`. Record the exact image digest used.
  This supersedes the earlier post-render-only preparation: `SetDefaults` now
  preserves omitted MRI replicas, the production/base CRDs remove that default,
  and newly created nil-parent children receive a positive one-replica bootstrap.
  **Do not use an old overlay with an old KAITO image as a substitute.**
- On a **disposable cluster**, install the prerequisite's actual CRDs and image.
  Read the stored parent from the API: *both* role replicas must be absent, not
  explicitly null, zero or defaulted to one. Prove this again after KAITO reconcile,
  webhook invocation and controller restart. Explicit fixed replicas must still
  retain their normal semantics.
- Bootstrap two healthy, positive-count GPU role pools. Before activation, use an
  operator-only child `/scale` write with no SO/HPA and prove KAITO preserves it
  through reconcile/restart. Restore the intended positive baseline. Do not change
  `resource.count`, TP, template, runtime config, node pool size or GPU allocation
  as part of autoscaling. This scaler never writes them or `/scale` itself.
- Only single-node, single-GPU-model-pod Workspaces are supported in this first
  path (`Workspace.status.targetNodeCount == 1`, one StatefulSet replica). The
  pod must be scheduled and have a positive `nvidia.com/gpu` limit. MIG, CPU,
  multi-node Ray, zero replicas and changing GPU/TP allocations are excluded.
- Pre-provision sufficient fixed GPU nodes. Disable node autoscaling/NAP expansion
  for this experiment. Pin node selectors/SKUs, model/runtime images by digest and
  role runtime configuration. Manual verification is required; this code does not
  prove cluster capacity or disable infrastructure controllers.
- Both roles must have healthy routing, KV transfer and model readiness under
  fixed replicas before any autoscaling test. Any such failure blocks activation.

## Freeze identities and policy

There is **no new MRI API field or parent annotation**. `KAITO_MRI_POC` is an
experimental process-local JSON allowlist. The chart exposes it only under
`mriPoc.enabled: true` / `mriPoc.config`. With the feature disabled, no additional
RBAC or environment variable is rendered. Malformed configuration fails startup.

Snapshot these exact resources into a private local directory (read-only API
operations; substitute your actual names):

```sh
NS=mri-poc
MRI=demo
mkdir -p snapshots
kubectl -n "$NS" get multiroleinference "$MRI" -o json > snapshots/parent.json
kubectl -n "$NS" get inferenceset "$MRI-prefill" -o json > snapshots/prefill.json
kubectl -n "$NS" get inferenceset "$MRI-decode" -o json > snapshots/decode.json
# Use each role's actual spec.roles[].runtimeConfig name:
kubectl -n "$NS" get configmap PREFILL_RUNTIME_CONFIG -o json > snapshots/prefill-runtime.json
kubectl -n "$NS" get configmap DECODE_RUNTIME_CONFIG -o json > snapshots/decode-runtime.json
go run ./hack/mri-poc-config ./snapshots > observation-config.json
```

The offline helper does not contact Kubernetes, write resources, or grant either
attestation. It emits parent/child UIDs, a parent-spec hash (except replica counts),
child-spec hashes (except replica counts), and runtime ConfigMap data hashes.
All are rechecked against uncached API reads at reconcile and metric time.
Admission defaulting must already have happened before snapshotting. Recreating a
parent or child, changing templates/runtime/topology, or losing controller owner
references invalidates authorization. Do not simply rehash an unexpected change.

Initial configuration has `activate: false`, `exclusiveWritersVerified: false`,
`proxyMetricsVerified: false`. It **cannot create/update a lab SO/HPA or serve
scaling metrics**. Inspection, direct endpoint verification and load observation
remain operator tasks until the following gates are complete.

## Exclusive writers: mandatory manual activation gate

A list-then-write check cannot prevent another actor from racing it. This code
rejects foreign/same-name/competing SOs and HPAs and never adopts or deletes them,
but it cannot block manual `/scale`, GitOps writes, KEDA pause annotations, a
lowered HPA maximum or an independent controller from changing replicas between
reads. Nor can withholding metrics undo a reduction already made by another writer.

Before setting `exclusiveWritersVerified: true`:

1. Use a dedicated lab cluster, one KEDA installation and one scaler installation.
2. Remove conflicting SOs/HPAs through their owners and verify their absence.
3. Establish and test RBAC/admission restrictions for the two child scales and
   SO/HPA objects. Only the intended KEDA/HPA chain may control replicas; deny
   manual/GitOps `/scale`, parent fixed replicas, alternate autoscalers, and
   unauthorized SO/HPA edits while active. Preserve KAITO's child reconciliation
   and template ownership. Audit the actual identities used in your cluster.
4. Specifically prevent HPA `maxReplicas` being lowered below live replicas,
   KEDA paused-replicas/ownership-transfer annotations, fallback/idle/restore
   activation, and competing targets. A disabled scale-down policy does **not**
   prevent HPA max clamping, direct scale writes or KEDA lifecycle actions.
5. Record admission/RBAC negative tests and audit evidence. If exclusions cannot
   be guaranteed, **stay observation-only; do not activate this POC**.

No admission restriction is silently installed by this patch. The optional chart
RBAC adds read-only MRI/workload/endpoint access and HPA read access in the chosen
namespace, with update permission only for the two named KEDA HPAs. It adds no
InferenceSet/Workspace scale permission. Review the pre-existing scaler/KEDA roles
as well: this chart is not an exclusive-writer security boundary.

## Prove role-local metrics before activation

Only the role's own model-Service HTTP endpoint is used:

| Role | Gauge | Required endpoint |
| --- | --- | --- |
| prefill | `vllm:num_requests_waiting` | `http://<workspace>.<namespace>.svc.cluster.local:80/metrics` |
| decode | `vllm:num_requests_running` | Same Service port 80 -> model pod port 5000 |

Decode may have a front proxy on pod port 5000 and an engine on pod port 5001.
**Pod port 5001 is not asserted to be a Service port.** Compare the Service/proxy
payload with the direct engine payload under controlled load (e.g. authorized
port-forward to the actual engine pod) and record that the gauge is forwarded
unchanged. Confirm exactly one correctly typed gauge series per model pod,
including appropriate `# TYPE ... gauge` exposition and no duplicate model/engine
series. Missing or ambiguous engine series are not combined silently.

Set `proxyMetricsVerified: true` only after both role endpoints, semantics and
identity are verified with actual deployed images. If Service `/metrics` does not
expose the engine gauge, stop; this patch does not modify the proxy or engine.

Every scrape is uncached and complete-member: enumerate owned Workspaces,
StatefulSets, ready GPU pods, exact Service selectors and ready EndpointSlices.
There must be exactly one ready endpoint referencing the expected pod UID/IP per
Workspace. The desired/actual/ready counts must agree. Fetch all role members,
then revalidate authorization, membership and resource versions. Use an arithmetic
average across **all** expected members; never average surviving responses.

A failed/missing/partial/duplicate/nonfinite/negative gauge, stale explicit sample
timestamp, timeout, rollout, pending pod, terminating member, mixed ownership,
endpoint mismatch or changing membership returns an error (hold), not zero. HTTP
requests have a 3-second timeout; the complete collection plus postchecks has a
30-second deadline (two 15-second KEDA intervals). There is no previous-window,
zero-replica, registry or router fallback. A response with no explicit Prometheus
timestamp is timed by this live collection; engine-side freshness still requires
the preceding deployment-specific verification.

## Child annotations and activation

Use the **existing child annotations only**, after KAITO has created the children.
Do not include guessed child UIDs in a parent template. Start with auto-provision
false. Apply one gauge per child; example prefill configuration:

```yaml
scaledobject.kaito.sh/auto-provision: "false"
scaledobject.kaito.sh/min-replicas: "1"
scaledobject.kaito.sh/max-replicas: "4"
scaledobject.kaito.sh/metrics: |
  - name: vllm:num_requests_waiting
    type: gauge
    source: modelpod
    aggregation: service-avg
    upthreshold: 10
    downthreshold: 2
```

For decode substitute `vllm:num_requests_running`. Thresholds are illustrative,
not tuned production values. Require `0 <= downthreshold < upthreshold`; the down
threshold is retained to use the existing parser but **cannot cause downscale**.
Min must be positive and <= the live count; max must be >= live count. No
activation/deactivation threshold, metric-cache window, node-count-derived bound,
zero fallback, idle replicas or restore-to-original is supported here.

After every prerequisite and writer-exclusion test passes, explicitly set all
three booleans in a reviewed copy of the JSON to true. Deploy your locally built
scaler image/digest through the normal chart flow, passing that JSON with
`--set mriPoc.enabled=true --set-file mriPoc.config=active-config.json` (plus the
normal installation values and image settings). Only then explicitly change the
two child auto-provision annotations to `"true"`. These are operator steps, not
commands this change has run.

The controller creates **only** an owned same-name SO for each authorized child.
The KEDA-generated HPA must be `keda-hpa-<child>` with a controller owner reference
to that SO UID. Both SO and HPA target the child InferenceSet, never the MRI, a
Workspace, StatefulSet, Deployment, router, registry or standalone service.

Generated policy:

- target metric type `Value`, composite target `1`;
- `gate == 1 && role_mean > upthreshold ? 2.0 : 1.0`;
- scale-up limited to one replica per 60 seconds;
- scale-down `selectPolicy: Disabled` (formula never requests below current);
- positive minimum; no fallback, idle, restore, pause or cached trigger behavior.

Readiness does not permit repeated growth while a prior member is pending: metric
collection requires full desired/actual/ready membership. The controller requeues
every 15 seconds and watches parent changes. `SelectPolicy` drift is compared and
repaired for correctly owned SO/HPA objects. Metrics additionally compare the live
SO to the exact generated child policy and the actual HPA bounds/behavior/composite
metric; unsafe drift returns unavailable until repaired. Repair is not a substitute
for excluding concurrent writers. The scaler never adopts a foreign SO/HPA and
never automatically deletes one when scope is invalidated.

## Required live acceptance and failure matrix

Record resource UIDs/versions, parent stored JSON, child/Workspace/pod membership,
SO and HPA JSON, gauge payloads, audit scale writes and a single UTC timeline.

1. Observation-only or absent config: no lab SO/HPA created and MRI gRPC values
   unavailable, including readiness/replica-count and legacy-cache requests.
2. Valid activation: correct child-target SO/HPA; no unrelated workload changes.
3. Prefill-only load: one prefill replica added, decode stable. Decode-only load:
   one decode replica added, prefill stable. Verify labels, routing and KV transfer.
4. Sustain load while a new member is Pending: no further growth until all members
   are ready. At max, no overshoot. Confirm GPU pool size/TP/templates unchanged.
5. Low/no load: **no replica reduction**. Check actual scaleDown Disabled and max
   bounds, not just formula text. Recheck after KEDA/scaler/KAITO restarts.
6. Remove a metric, fail one endpoint, inject an old timestamp/nonfinite value,
   duplicate a series, restart a member, alter Service/EndpointSlice membership,
   trigger rollout or make a GPU unavailable: unavailable/hold, never a partial mean.
7. Attempt forged/wrong namespace/UID/owner target, parent fixed replicas,
   replacement parent/child, altered runtime/template, foreign SO/HPA, paused
   replicas, fallback, idle, restore and lowered max: deny or hold. Admission must
   block the writer before it can change replicas. Do not demonstrate destructive
   bypasses with live user traffic.
8. Change only child replica count through the authorized HPA path: KAITO leaves
   it intact across reconcile/restart while retaining template/spec ownership.

Unit/fake-client tests do not prove real API persistence, admission, KEDA behavior,
HPA controller scheduling, GPU inference, proxy metrics, routing, KV transfer,
cluster capacity, model warmup or performance. No live result is implied here.

## Stop and return to fixed replicas safely

1. Stop test traffic and exclude/fence scale writers using the verified lab control.
2. Set child auto-provision annotations false and stop/disable the dedicated scaler
   process. This does not itself delete SO/HPA objects or undo prior writes.
3. Inspect both owned SOs: no idle/fallback/restore/paused-replicas behavior. Through
   an authorized operator, delete only the two verified owned SOs and their owned
   HPAs, respecting GC/finalizers. **Verify both SO and HPA absent**. Do not delete
   unrelated objects or blindly force finalizers.
4. Only after writer shutdown and absence verification, set explicit positive
   parent role replicas. Let KAITO take back fixed counts and verify stability.
5. Restore ordinary RBAC/admission and retire the disposable deployment only after
   the ownership handoff is complete. Automatic zero/downscale, draining, router
   modifications, rollback by paused replicas and infrastructure resizing are not
   part of this POC.
