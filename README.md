# Sandbox benchmarks

Pick a provider, supply your own credentials, and run the same scripted checks. The adapters cover **Tensorlake, Modal, Boat, Blaxel, Beam VMs and exe.dev**. Each run gets its own results and resource ledger.

This started as our Tensorlake benchmark. The [original controller and setup instructions](docs/tensorlake-reference.md) remain available for reproducing that protocol. The portable runner reuses its app, browser, S3 and saved-state workload. It does not rewrite historical results.

To get the numbers behind each chart and table in [Choosing a sandbox provider](https://cofounder.co/research/sandbox-providers), see [Reproduce the published results](#reproduce-the-published-results).

## Get started

Use Python 3.11–3.13. Install the provider you want:

```sh
git clone https://github.com/General-Intelligence-Company/sandbox-benchmarks.git
cd sandbox-benchmarks
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[tensorlake]'
sandbox-bench init --provider tensorlake
```

Set `TENSORLAKE_API_KEY` in your environment, then:

```sh
sandbox-bench doctor --config benchmark.toml
sandbox-bench run --config benchmark.toml
```

`doctor` is local only. It checks the configuration, installed SDK and presence of the required variables without showing their values. **`run` creates billable resources.** The generated config starts with five sandboxes and the command smoke suite. It checks terminal access and basic file operations, then deletes the sandboxes. It is not the historical 20-command throughput test or the full browser/S3 workload.

For another provider, change the extra and generated config:

```sh
pip install -e '.[blaxel]'
sandbox-bench init --provider blaxel --output blaxel.toml
# Set BL_API_KEY and BL_WORKSPACE in your environment.
sandbox-bench doctor --config blaxel.toml
sandbox-bench run --config blaxel.toml
```

Boat and exe.dev need no SDK extra. `sandbox-bench providers` lists credentials and the exact cases supported by each adapter. See [provider setup](docs/providers.md) for image requirements, permissions, preview access and cleanup limitations. No Doppler account or company-specific infrastructure is required.

For a Tensorlake retest in batches of 500 within a 1,000-sandbox organization limit,
set `batch_size = 500`, `customer_headroom = 500`, and
`organization_sandbox_limit = 1000` at the top level of the config. This opt-in
admission check requires the authenticated Tensorlake `tl` CLI on `PATH`, using
the same `TENSORLAKE_API_KEY`. It reads organization-wide running usage before
preparation and each new batch, outside measured timings. Insufficient capacity
stops dispatch and keeps unrun cases visible; it does not raise quotas or replay
cases. Tensorlake's completed snapshots outlive their sources, so tools and
workload source VMs are deleted and their absence confirmed before each measured
batch. Preparation sources therefore do not add to the 500 resident benchmark VMs.
Same-ID pause/resumes use the configured bounded concurrency.

## Run the full workload

Set `suite = "workflow"` in your config and fill in its `[s3]` section. Use an existing disposable test bucket, an existing seed object and a temporary AWS role profile. The controller verifies the AWS account before any test writes. That role needs to list the configured prefix, read the seed, and read/write/delete the run's objects. Provider control-plane keys never enter the guest.

The workload runs these steps in each sandbox:

1. Execute a terminal command and verify a local file write.
2. Mount the configured S3 prefix with s3fs and read the seed.
3. Clone the included Git bundle, change the app and install its dependencies.
4. Start the app, open Chromium and verify a real interaction.
5. Write through the S3 mount, then independently read and check the bytes from the controller.
6. Run a common 60-second activity window with 12 app, browser, network and S3 checks.

CPU/RAM samples, command outcomes, screenshots and guest logs are collected. No LLM or model API key is needed. A full workflow is only a pass after the outside S3 readback succeeds. A clean activity window is scored separately.

For example, Tensorlake supports all four cases:

```toml
provider = "tensorlake"
suite = "workflow"
count = 10
batch_size = 5
concurrency = 5
lifecycles = ["fresh", "disk", "memory", "resume"]

[s3]
profile = "my-benchmark-role"
expected_account = "123456789012"
bucket = "my-sandbox-test-bucket"
region = "us-east-1"
prefix = "sandbox-bench/tensorlake/"
seed = "readme.txt"
```

`resume` reuses eligible fresh sandboxes. Disk and memory tests prepare a source outside the measured cohort, then fork new IDs. Failed cases are not replaced. Unsupported cases fail configuration validation rather than silently using a different operation.

**`count` is the total per lifecycle, not the concurrent VM count.** `batch_size` limits resident measured sandboxes; `concurrency` limits simultaneous trial setup. A snapshot source adds one resident VM. The defaults are deliberately small. Keep the total below your account capacity and leave room for other users. This runner does not reserve organization-wide capacity against other clients.

Temporary AWS credentials must have at least 25 minutes left before a batch starts. They are not refreshed inside saved memory. If they expire or a quota blocks admission, the run stops, retains unfinished counts and cleans up what it can. There is no automatic continuation or replay of an interrupted lifecycle operation.

## Reproduce the published results

[Choosing a sandbox provider: lessons from 13,000 launches](https://cofounder.co/research/sandbox-providers) reports results from this harness. This section lists the run behind each figure and table, and where each published number appears in the output.

These runs create a lot of billable resources: 100 sandboxes per lifecycle at N=100 and 1,000 per lifecycle at N=1,000, plus preparation and snapshot sources. Your numbers will differ from ours. Region, where the controller runs, account limits and the day all move them, and provider calls include the network round trip from your controller.

| Published result | Run | Where the numbers are |
| --- | --- | --- |
| Figure 1: time until the whole workflow works | N=100 | `terminal_ready_s`, `local_write_ready_s`, `s3_verified_s` and `full_workflow_s` in `summary.json` |
| Figure 2: provider call time | Same N=100 run | `provider_lifecycle_s`, with `harness_setup_s`, `guest_workflow_s` and `external_verification_s` in its exact-values table |
| Reliability table | Same N=100 run | `clean_activity` out of `requested` |
| Figure 3: 1,000 sandboxes | N=1,000, as several runs | Trial rows pooled across those runs |
| Figure 4 and the concurrent-command check | Not this harness | See [What this harness does not produce](#what-this-harness-does-not-produce) |

Every published workflow run used prepared tools and the same requested shape, with an `[s3]` section as described in [Run the full workload](#run-the-full-workload):

```toml
suite = "workflow"
prepared = true
cpus = 4
memory_mb = 8192
disk_mb = 20480
```

We ran the SDK versions pinned in `pyproject.toml`. The Tensorlake N=1,000 run used commit `77901cc`; later commits on `main` change only documentation.

### N=100: Figures 1 and 2 and the reliability table

Run one config per provider with `count = 100` and `batch_size = 100`, and these lifecycles and setup concurrency:

| Provider | `lifecycles` | `concurrency` | Account prerequisites |
| --- | --- | --- | --- |
| Tensorlake | `["fresh", "disk", "memory", "resume"]` | 64 | Memory snapshot access. We also set `customer_headroom = 500` and `organization_sandbox_limit = 1000`. |
| Modal | `["fresh", "disk", "memory"]` | 64 | Memory snapshot access. |
| Blaxel | `["fresh", "memory"]` | 64 | Snapshot preview access. |
| Beam VMs | `["fresh", "disk", "resume"]` | 64 | A workspace limit above 400 vCPUs. Ours was raised to 1,000 after 100 stopped the first attempt. |
| exe.dev | `["fresh", "disk", "resume"]` | 8 | A key that allows `new`, `ls`, `ssh`, `cp`, `pause`, `resume` and `rm`. |
| Boat | `["fresh", "resume"]`, then `["disk"]` as a second run | 3 | Enough start allowance. Ours was 420 active sandboxes and 42 starts per minute. |

For example, the Tensorlake config:

```toml
provider = "tensorlake"
suite = "workflow"
prepared = true
count = 100
batch_size = 100
concurrency = 64
customer_headroom = 500
organization_sandbox_limit = 1000
lifecycles = ["fresh", "disk", "memory", "resume"]
cpus = 4
memory_mb = 8192
disk_mb = 20480
ttl_seconds = 3600

[s3]
profile = "my-benchmark-role"
expected_account = "123456789012"
bucket = "my-sandbox-test-bucket"
region = "us-east-1"
prefix = "sandbox-bench/tensorlake/"
seed = "readme.txt"
```

The two capacity settings need the authenticated `tl` CLI described in [Get started](#get-started). Leave them out if nobody else uses your Tensorlake organization.

Each lifecycle's results are under `cases.<lifecycle>` in `summary.json`. The published mean and P95 are the `mean` and `p95` of each field under `timings_s`. They cover successful workflows only, and `n` is the sample count.

| Published column | Field |
| --- | --- |
| Terminal ready | `timings_s.terminal_ready_s` |
| Disk writable | `timings_s.local_write_ready_s` |
| S3 verified | `timings_s.s3_verified_s` |
| Workflow complete | `timings_s.full_workflow_s` |
| Provider call | `timings_s.provider_lifecycle_s` |
| Harness setup | `timings_s.harness_setup_s` |
| Guest workflow | `timings_s.guest_workflow_s` |
| Outside S3 readback | `timings_s.external_verification_s` |
| Reliability: passed every check for the full minute | `clean_activity` out of `requested` |
| Reliability: completed the workflow, such as Beam resume's 38 of 100 | `passed` out of `requested` |

Two exact-value columns are not in `summary.json`. The clock alignment interval in Figure 1's table is the width of each trial's guest/controller clock bound on the disk-writable milestone. The scheduler overlap in Figure 2's table is the controller event-loop lateness that fell inside the provider call. It is an upper bound and is never subtracted from other timings. With the virtual environment active, print both for one lifecycle of a run:

```sh
python3 - fresh outputs/tensorlake-<run-id> <<'EOF'
import json, statistics, sys
from pathlib import Path
from sandbox_bench.runner import Evidence

case, run = sys.argv[1:]
rows = [
    r for r in Evidence.read_state(run)["trials"]
    if r["case"] == case and r["status"] == "passed"
]
loop = json.loads((Path(run) / "controller-observations.json").read_text())["event_loop"]
clock = [
    r["milestone_observation_bounds_s"]["local_write"]["latest"]
    - r["milestone_observation_bounds_s"]["local_write"]["earliest"]
    for r in rows
]
overlap = [
    sum(
        max(0, min(s["received"], r["lifecycle_observation"]["received_monotonic"])
            - max(s["scheduled"], r["t0"]))
        for s in loop
    )
    for r in rows
]
print(f"clock alignment interval mean={statistics.mean(clock):.2f}")
print(f"scheduler overlap mean={statistics.mean(overlap):.2f}")
EOF
```

The November 18 runs were scored before `activity_protocol` was recorded. A rerun scores the activity minute with `monotonic-interval-v1`, so its clean-minute counts can differ slightly from ours.

### N=1,000: Figure 3

We ran N=1,000 as several smaller runs and pooled their successful trials:

| Provider | Runs | Settings in each run |
| --- | --- | --- |
| Tensorlake | Four runs, each with all four lifecycles | `count = 250`, `batch_size = 250`, `concurrency = 64`, `customer_headroom = 500`, `organization_sandbox_limit = 1000`, `ttl_seconds = 1800` |
| Modal | Four runs per lifecycle, one lifecycle per run | `count = 250`, `batch_size = 250`, `concurrency = 64`, `ttl_seconds = 1200` |
| Boat | Runs of 100 until each lifecycle reached 1,000, over November 19–23 to stay inside the start allowance | `count = 100`, `batch_size = 100`, `concurrency = 3` |

Give each campaign its own `--output` directory, so a glob picks up only its runs. Then pool one timing field for one lifecycle:

```sh
python3 - full_workflow_s fresh outputs/n1000-tensorlake/tensorlake-* <<'EOF'
import math, statistics, sys
from sandbox_bench.runner import Evidence

field, case, *runs = sys.argv[1:]
values = sorted(
    r[field]
    for run in runs
    for r in Evidence.read_state(run)["trials"]
    if r["case"] == case and r["status"] == "passed" and r.get(field) is not None
)
p95 = values[math.ceil(len(values) * 0.95) - 1]
print(f"n={len(values)} min={values[0]:.2f} mean={statistics.mean(values):.2f} "
      f"p95={p95:.2f} max={values[-1]:.2f}")
EOF
```

Figure 3's three views are `terminal_ready_s` for `fresh`, `full_workflow_s` for `fresh`, and `full_workflow_s` for `memory`. The command prints the minimum, mean, P95 and maximum shown in its exact-values table. It works for any timing field and lifecycle, including a single N=100 run.

### What this harness does not produce

- Figure 4, 20 sequential commands in each of 100 disk-snapshot sandboxes, ran with a separate script on October 23 for Tensorlake and Modal and November 2 for Boat. That script is not in this repository yet, and `suite = "commands"` is a smoke test, not that benchmark.
- The five-concurrent-command check is not in this repository either.
- The Vercel row of the reliability table came from a separate Vercel runner. This repository has no Vercel adapter.
- The Blaxel resume row, a wake from automatic standby, used the unmerged `codex/blaxel-resume-sup-8181` branch. The Blaxel adapter on `main` supports `fresh` and `memory` only.
- The provider comparison table comes from provider documentation and from the nine-part coding and seven-part library tests, which ran as separate agent sessions rather than with this harness.
- The cost charts are arithmetic on October 2026 list prices, not measurements. Snapshot cost is 10,000 snapshots × 10 GB × the monthly per-GB rate, or (10,000 − 10 included) × $1.70 per snapshot for Boat. Compute cost is 100,000 hours × the hourly rate of each provider's closest shape to 4 vCPU and 8 GB. Check current prices before reusing them.

## Read the results

The command prints the run directory, for example `outputs/tensorlake-<run-id>/`.

```sh
sandbox-bench report outputs/tensorlake-<run-id>
sandbox-bench cleanup outputs/tensorlake-<run-id>
```

Tensorlake cleanup closes command connections and processes at most 16 VMs at a
time, with a 90-second deadline per VM. Timed-out deletions are recorded once;
they are not automatically repeated. After inspecting the exact run-owned IDs,
an operator can explicitly authorize one new delete with
`sandbox-bench cleanup RUN_DIR --confirm-delete OWNED_ID ...`. Unknown IDs are
rejected. An independent absence check reconciles the old ambiguous record while
preserving its original error. Cleanup's exit code reports resource cleanup,
independently of whether the measured workflows passed.

- `run.json`: a checkpoint of configuration, lifecycle intents/results, ownership and per-case outcomes.
- `updates/`: atomic trial and operation records written during dispatch. Keep this directory with `run.json`; `report` and `cleanup` combine both when a run is active or interrupted.
- `summary.json`: attempted, passed, failed, ineligible and unrun counts; successful workflow mean/P95; activity and cleanup coverage.
- `events.jsonl`: command timing/outcomes and each independent S3 read attempt.
- `controller-observations.json`: event-loop lateness samples and local evidence-write intervals, using the same controller monotonic clock as command and lifecycle observations.
- `artifacts/`: guest logs, resource samples and browser screenshots.

Readiness milestones are cumulative from the create/fork/resume dispatch. Terminal ready means a command ran. Local-disk, S3 and full-workflow readiness are separate checkpoints. Do not add their times together. Queue waiting, activity and cleanup are separate.

Dispatch writes only the changed record. Full history checkpoints happen outside measured workflows, so evidence from earlier batches does not delay later launches.

Activity windows use one future controller-monotonic start, translated through a fresh clock sample from each guest. Request/response times bound the clock offset without assuming symmetric network latency. The guest schedules against its monotonic clock, so a restored wall clock cannot trigger catch-up bursts. Late arm acknowledgements, clock discontinuities and alignment uncertainty above one second cannot pass. Scoring uses the latest possible event time within the clock bounds. A clock interval that straddles a deadline is reported as unobserved timing, not a provider cadence failure. The lead, calibration and collection stay outside workflow timing; late checks remain failures.

`activity_protocol = "monotonic-interval-v1"` identifies this activity scoring. Native lifecycle and workflow timing definitions remain unchanged. `activity_functional`, `activity_cadence_miss`, `activity_unarmed`, `activity_unobserved` and `activity_unknown` explain the strict `clean_activity` count without relaxing it. Control traffic is paced separately from measured lifecycle and terminal calls. Only read-only requests are retried, within an overall deadline; an ambiguous activity arm or lifecycle mutation is never replayed.

Workflow runs default to `prepared = true`. They install tools and stage benchmark files once in a separate preparation sandbox, then create a prepared base before dispatching measured cases. A measured case verifies the staged file hashes and fails if the prepared files are missing; it never silently installs tools. Set `prepared = false` to measure the cold-install protocol instead. Keep the protocols separate when comparing results.

Prepared bases use each provider's native disk image/snapshot support. Blaxel uses its image builder. exe.dev uses a disk copy of a stopped tools-only source, so its prepared `fresh` case measures that creation path, not `new` from a registry image. The preparation method, reference and duration remain in `prepared_tools`; workload disk and memory forks still use separate sources containing the running app's state. All run-owned bases and source VMs are included in cleanup.

Fresh rows record `source_kind` and `case_label`: `prepared_snapshot` for a prepared snapshot/copy, `prepared_image` for a prepared image, or `image` for cold image creation. These labels also appear in the summary. New workflow runs use `prepared-v2` or `cold-v2`; existing run files are never migrated or overwritten.

The [prepared-image N=5 validation](docs/prepared-validation-2026-11-13.md) includes per-case timings and the preparation failures found during live execution.

### Timing fields

| Field | Includes |
| --- | --- |
| `provider_lifecycle_s` | The native create/fork/resume call, including network transit and SDK waits. Local journal writes and tools preparation are outside this clock. This is an observed API duration, not internal provider boot time. |
| `terminal_ready_s` | Time from native dispatch until the first successful terminal command is received. Every failed readiness probe and its send/receive bounds are recorded. |
| `s3_write_ready_s` | Guest S3-write completion, translated to the upper edge of the clock-alignment interval. It excludes later state capture and independent readback. |
| `s3_verified_s` | Cumulative time until the controller confirms the written bytes in S3. Currently the same final milestone as `full_workflow_s`. |
| `harness_setup_s` | Prepared-file checks, guest-service startup and saved-state observation before the workload; cold installation when requested. |
| `guest_workflow_s` | Monotonic execution duration measured on the guest's workload thread. It excludes the native exec transport and outside S3 verifier. |
| `workflow_rpc_s` | Complete workload RPC observation on the controller, including native command transport and response decoding. |
| `workflow_observation_overhead_s` | Workload RPC duration minus guest execution duration. This contains transport, command launch and queueing; it is a measured residual, not a correction subtracted from other clocks. |
| `external_verification_s` | Independent S3 readback, including recorded retries. |
| `full_workflow_s` | The complete observed duration from dispatch through S3 verification, including harness setup. It stays available alongside the separated measurements. |
| `admission_wait_s` | Local wait for a trial slot, before native dispatch and outside provider/workflow clocks. |
| `dispatch_preparation_s` | Local preparation and evidence recording after admission, before native dispatch. |
| `post_lifecycle_recording_s` | Controller bookkeeping between the native lifecycle response and the first terminal probe. Included in cumulative readiness, outside `provider_lifecycle_s`. |

`guest_milestones_s` records each step relative to guest execution start. `milestone_observation_bounds_s` brackets cumulative guest milestones using the terminal probe's send/receive times; cross-machine clock alignment is not treated as exact. Do not subtract an assumed network round trip or half a polling interval to manufacture provider-only latency.

Controller observations measure event-loop lateness every 100 ms and evidence serialization/write intervals in memory, then save them after cleanup, outside measured workflows. Command events and lifecycle rows record send/receive times. Each external S3 read records executor queue time, worker duration and return-to-event-loop delay. These observations expose local contention; they are not subtracted from raw timings. The final observation-file write and final checkpoint occur after sampling ends.

Providers return from creation at different states: some acknowledge a new ID while others wait for a running machine. Use `terminal_ready_s` for the common request-to-working-command comparison; do not rank provider startup by `provider_lifecycle_s` alone.

Saved-state checks exercise the preserved browser, app and filesystem, including S3. Time spent in those checks is included in `harness_setup_s` and the end-to-end result. It can reflect real saved-session usability delays, not just controller overhead, so do not discard it or treat `guest_workflow_s` alone as time to a fully usable restored session.

Readiness retries use a 100 ms interval after a failed command. Authentication and rate-limit responses stop observation. Tensorlake resume uses 100 ms SDK polling; its normally blocking create also uses 100 ms if the SDK falls back to polling after the server-side wait budget expires. Native command waits are used where available; Beam may return a running PID even with `wait=True`, so the runner observes that same PID at 100 ms and logs every observation without replaying the command. Faster polling reduces observation delay but still consumes API capacity; control concurrency for your account's limits.

The S3 verifier retries only connection failures and `SlowDown`, up to three total attempts within a 15-second budget. Every attempt stays in the evidence. Authentication errors and content mismatches are not retried. Provider lifecycle writes are submitted once by the controller; provider SDK internal retries, where applicable, are described in [provider setup](docs/providers.md).

A missed activity check is not proof of a crashed VM. The report separates guest results, outside S3 readbacks, cadence misses, collection failures and unknown causes. Timings summarize successful workflows only; always read their sample count beside failure counts.

## Cleanup and interrupted runs

Cleanup acts only on IDs created by this run, exact names from its create intents, and its generated S3 subprefix. It independently checks VM deletion. A second controller cannot lock the same run directory. There is no `resume` command that might repeat an ambiguous create or fork.

Keep the run directory after an interruption. `cleanup` can reconcile named resources and remove known IDs without starting a replacement cohort. Lost snapshot responses, incomplete provider inventories and SDK deletion limitations remain visible; the tool does not call those fully reconciled. Modal memory snapshots must expire under the provider's retention policy. Cached base images are not removed.

Evidence export gets at most three attempts within 120 seconds. Interruption also allows a bounded final collection attempt before cleanup. Missing archives are recorded explicitly and do not keep billable VMs alive indefinitely. Do not manually delete run-owned VMs before checking archive coverage or attempting rescue. S3 cleanup waits until owned writers are confirmed absent. Asynchronous snapshot deletion is polled without resubmitting it; Modal memory snapshots remain `provider_expiry_pending` with an expected expiry bound, never falsely marked deleted.

Outputs can contain app files and environment-specific metadata. Review them before sharing. Keys and signed URLs are redacted from JSON logs, and provider keys are never copied into the guest.

## Development

```sh
pip install -e '.[test,tensorlake,modal,blaxel,beam]'
pytest -q
python3 check_source_integrity.py
python3 controller/check_s3_read_retries.py
```

These checks do not start cloud resources. The adapters have pinned-SDK and offline regression tests. Live N=5 workflow validation now covers all six adapters: Boat, Tensorlake, Beam VMs, Modal, Blaxel and exe.dev. See the [November 13 validation and timing audit](docs/live-validation-2026-11-13.md), including the fixes, retained first attempts and cleanup limitations. The [November 12 results](docs/live-validation-2026-11-12.md) remain available. Run a small test in your own account before scaling.

The shared workload lives in `context/`; provider operations are in `sandbox_bench/providers.py`; orchestration and evidence handling are in `sandbox_bench/runner.py`. Add another adapter without copying the workload. The original controller, source manifest and historical artifacts remain separate.
