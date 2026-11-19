"""Run-owned resources, first-attempt evidence and a common guest workload."""

import asyncio
import base64
import fcntl
import hashlib
import io
import json
import math
import os
import random
import re
import shlex
import statistics
import tarfile
import time
import uuid
from datetime import datetime, timezone
from importlib.metadata import distribution
from pathlib import Path
from email.utils import parsedate_to_datetime

import httpx

from .activity import ClockAlignment, activity_category, score_activity
from .providers import ProviderError
from .tools import INSTALL


class GuestFailure(RuntimeError):
    def __init__(self, result):
        super().__init__("Guest workload: " + json.dumps(result))
        self.result = result


def now():
    return datetime.now(timezone.utc).isoformat()


def http_status(error):
    return (
        getattr(error, "status_code", None)
        or getattr(error, "status", None)
        or getattr(getattr(error, "response", None), "status_code", None)
    )


def assets():
    local = Path(__file__).resolve().parents[1] / "context"
    if local.is_dir():
        return local
    package = distribution("sandbox-bench")
    bundle = next(p for p in package.files if p.name == "shared-app.bundle")
    return Path(package.locate_file(bundle)).resolve().parent


def python(command):
    return "python3 -c " + shlex.quote(command)


class Evidence:
    def __init__(self, path, state=None):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=state is None, mode=0o700)
        self.lock = (self.path / ".lock").open("a")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.state = state if state is not None else self.read_state(self.path)
        self.sequence = self.state.get("journal_sequence", 0)
        self.io_observations = []
        self.secrets = [
            v
            for k, v in os.environ.items()
            if v and ("TOKEN" in k or "KEY" in k or "SECRET" in k)
        ]

    def clean(self, value):
        def redact(value):
            if isinstance(value, str):
                for secret in self.secrets:
                    value = value.replace(secret, "[redacted]")
                return re.sub(
                    r'(https?://[^\s"?]+)\?[^\s"]+', r"\1?[redacted]", value
                )
            if isinstance(value, dict):
                return {redact(k): redact(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [redact(v) for v in value]
            return value

        return json.dumps(redact(value), default=lambda value: redact(str(value)))

    @staticmethod
    def read_state(path):
        path = Path(path)
        state = json.loads((path / "run.json").read_text())
        checkpoint = state.get("journal_sequence", 0)
        updates = [json.loads(p.read_text()) for p in (path / "updates").glob("*.json")]
        for update in sorted(updates, key=lambda item: item["sequence"]):
            if update["sequence"] <= checkpoint:
                continue
            value = update["value"]
            kind = update["kind"]
            rows = state["trials" if kind == "trial" else "operations"]
            identity = lambda row: (
                (row["case"], row["index"]) if kind == "trial" else row["operation_id"]
            )
            index = next(
                (i for i, row in enumerate(rows) if identity(row) == identity(value)),
                None,
            )
            if index is None:
                rows.append(value)
            else:
                rows[index] = value
            if kind == "operation" and value["status"] == "returned":
                if value["kind"] == "create":
                    state["owned"].setdefault(
                        value["result"], {"name": value["name"], "deleted": False}
                    )
                elif value["kind"] == "snapshot" and not any(
                    ref["id"] == value["result"]["id"] for ref in state["snapshots"]
                ):
                    state["snapshots"].append(
                        {**value["result"], "source_id": value["id"]}
                    )
            state["journal_sequence"] = update["sequence"]
        return state

    def write(self, path, value):
        started = time.monotonic()
        temp = path.with_suffix(path.suffix + ".tmp")
        temp.write_text(self.clean(value) + "\n")
        temp.replace(path)
        self.io_observations.append(
            {"kind": "write", "started": started, "finished": time.monotonic()}
        )

    def save(self):
        # Full checkpoints run between measured phases. Dispatch writes only the
        # changed record, so an old cohort cannot delay the next provider call.
        self.state["journal_sequence"] = self.sequence
        self.write(self.path / "run.json", self.state)

    def save_update(self, kind, key, value):
        self.sequence += 1
        directory = self.path / "updates"
        directory.mkdir(exist_ok=True)
        self.write(
            directory / f"{kind}-{key}.json",
            {"sequence": self.sequence, "kind": kind, "value": value},
        )

    def save_trial(self, row):
        self.save_update("trial", f"{row['case']}-{row['index']}", row)

    def log(self, kind, **value):
        started = time.monotonic()
        with (self.path / "events.jsonl").open("a") as stream:
            stream.write(self.clean({"at": now(), "kind": kind, **value}) + "\n")
        self.io_observations.append(
            {"kind": "log", "started": started, "finished": time.monotonic()}
        )

    async def mutation(self, kind, fn, **context):
        op = {
            "operation_id": uuid.uuid4().hex,
            "kind": kind,
            "at": now(),
            "status": "dispatched",
            **context,
        }
        self.state["operations"].append(op)
        self.save_update("operation", op["operation_id"], op)
        start = time.monotonic()
        try:
            value = await fn()
            op.update(status="returned", result=value)
            return value
        except BaseException as exc:
            op.update(status="unconfirmed", error=type(exc).__name__ + ": " + str(exc))
            raise
        finally:
            op["duration_s"] = time.monotonic() - start
            self.save_update("operation", op["operation_id"], op)


class S3Fixture:
    def __init__(self, config, evidence):
        import boto3
        from botocore.config import Config

        session = boto3.Session(
            profile_name=config["profile"], region_name=config["region"]
        )
        credentials = session.get_credentials()
        frozen = credentials.get_frozen_credentials()
        self.expiry = getattr(credentials, "_expiry_time", None)
        if self.expiry is None:
            raise ValueError(
                "AWS profile must return temporary credentials with an expiration time"
            )
        self.ensure_valid()
        if not frozen.token:
            raise ValueError(
                "Workflow tests require temporary AWS role credentials with a session token"
            )
        account = session.client("sts").get_caller_identity()["Account"]
        if account != config["expected_account"]:
            raise ValueError("AWS account does not match s3.expected_account")
        self.config = config
        self.evidence = evidence
        self.client = session.client(
            "s3",
            config=Config(
                connect_timeout=2, read_timeout=2, retries={"total_max_attempts": 1}
            ),
        )
        self.env = {
            "AWS_ACCESS_KEY_ID": frozen.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
            "AWS_SESSION_TOKEN": frozen.token,
            "AWS_DEFAULT_REGION": config["region"],
        }
        evidence.secrets.extend((frozen.access_key, frozen.secret_key, frozen.token))
        # Read the user's existing seed. Never overwrite it or create a shared fixture.
        seed = self.client.get_object(
            Bucket=config["bucket"], Key=config["prefix"] + config["seed"]
        )["Body"]
        try:
            data = seed.read()
        finally:
            seed.close()
        self.seed_hash = hashlib.sha256(data).hexdigest()
        self.env["N100_PROBE_URL"] = self.client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": config["bucket"],
                "Key": config["prefix"] + config["seed"],
            },
            ExpiresIn=3600,
        )
        evidence.secrets.append(self.env["N100_PROBE_URL"])
        evidence.state.setdefault("credential_checks", []).append(
            {"at": now(), "expires_at": self.expiry.isoformat(), "account": account}
        )

    def ensure_valid(self):
        if self.expiry.timestamp() - time.time() < 1500:
            raise RuntimeError(
                "AWS session has less than 25 minutes remaining; refresh it before a new run"
            )

    async def verify(self, write):
        queued = time.monotonic()

        def read():
            started = time.monotonic()
            from botocore.exceptions import (
                ClientError,
                ConnectionClosedError,
                ConnectTimeoutError,
                EndpointConnectionError,
                ReadTimeoutError,
                ResponseStreamingError,
            )

            attempts = []
            deadline = time.monotonic() + 15
            for attempt in range(1, 4):
                start, retry = time.monotonic(), None
                try:
                    r = self.client.get_object(
                        Bucket=self.config["bucket"], Key=write["key"]
                    )
                    body = r["Body"]
                    try:
                        data = body.read()
                    finally:
                        body.close()
                    result = {
                        "passed": len(data) == write["bytes"]
                        and hashlib.sha256(data).hexdigest() == write["sha256"],
                        "request_id": (r.get("ResponseMetadata") or {}).get(
                            "RequestId"
                        ),
                    }
                except Exception as exc:
                    result = {
                        "passed": False,
                        "error": str(exc),
                        "type": type(exc).__name__,
                    }
                    if (
                        isinstance(exc, ClientError)
                        and (exc.response.get("Error") or {}).get("Code") == "SlowDown"
                    ):
                        retry = "s3_slowdown"
                    elif isinstance(
                        exc,
                        (
                            ConnectionClosedError,
                            ConnectTimeoutError,
                            EndpointConnectionError,
                            ReadTimeoutError,
                            ResponseStreamingError,
                        ),
                    ):
                        retry = "transport"
                attempts.append(
                    {
                        **result,
                        "attempt": attempt,
                        "duration_s": time.monotonic() - start,
                        "retry_reason": retry,
                    }
                )
                if result["passed"] or not retry or attempt == 3:
                    break
                delay = (
                    random.uniform(0, 0.5) if retry == "s3_slowdown" else 0.25
                ) * 2 ** (attempt - 1)
                if time.monotonic() + delay + 4 > deadline:
                    break
                time.sleep(delay)
            return {
                **result,
                "attempts": attempts,
                "first_attempt_passed": attempts[0]["passed"],
                "worker_started": started,
                "worker_finished": time.monotonic(),
            }

        result = await asyncio.to_thread(read)
        received = time.monotonic()
        started, finished = result.pop("worker_started"), result.pop("worker_finished")
        result.update(
            thread_queue_s=started - queued,
            worker_duration_s=finished - started,
            thread_return_delay_s=received - finished,
        )
        self.evidence.log("external_s3_read", key=write["key"], **result)
        return result

    async def cleanup(self, run_id):
        prefix = self.config["prefix"] + "n100-" + run_id + "/"

        def remove():
            keys = [
                o["Key"]
                for page in self.client.get_paginator("list_objects_v2").paginate(
                    Bucket=self.config["bucket"], Prefix=prefix
                )
                for o in page.get("Contents", [])
            ]
            errors = []
            for start in range(0, len(keys), 1000):
                errors += self.client.delete_objects(
                    Bucket=self.config["bucket"],
                    Delete={
                        "Objects": [{"Key": k} for k in keys[start : start + 1000]]
                    },
                ).get("Errors", [])
            remaining = self.client.list_objects_v2(
                Bucket=self.config["bucket"], Prefix=prefix
            ).get("KeyCount", 0)
            return {
                "prefix": prefix,
                "deleted": len(keys),
                "remaining": remaining,
                "errors": errors,
            }

        return await asyncio.to_thread(remove)


class Runner:
    def __init__(self, config, provider, evidence, fixture=None):
        self.c, self.p, self.e, self.s3 = config, provider, evidence, fixture
        self.slots = asyncio.Semaphore(config["concurrency"])
        self.run_id = self.e.state["run_id"]
        self.loop_observations = []
        self.control_slots = asyncio.Semaphore(provider.control_concurrency)
        self.next_control_request = 0.0

    @staticmethod
    def retryable_control_error(exc):
        return http_status(exc) in (429, 500, 502, 503, 504) or isinstance(
            exc, (TimeoutError, httpx.TransportError, ConnectionError)
        )

    async def control(
        self, fn, deadline, *, retry=True, call_timeout=30, max_attempts=None
    ):
        """Pace unmeasured control traffic; only repeat explicitly read-only calls."""
        delay = 0.5
        attempts = 0
        while True:
            try:
                async with asyncio.timeout_at(deadline):
                    async with self.control_slots:
                        # Reservations run without an await, so concurrent tasks
                        # cannot reserve the same request time on this event loop.
                        at = max(time.monotonic(), self.next_control_request)
                        self.next_control_request = (
                            at + 1 / self.p.control_requests_per_second
                        )
                        await asyncio.sleep(max(0, at - time.monotonic()))
                        remaining = deadline - time.monotonic()
                        attempts += 1
                        async with asyncio.timeout(min(call_timeout, remaining)):
                            # Native SDK timeout schemas use integer seconds. The
                            # enclosing timer still enforces the precise deadline.
                            return await fn(max(1, math.ceil(min(call_timeout, remaining))))
            except Exception as exc:
                if (
                    not retry
                    or not self.retryable_control_error(exc)
                    or time.monotonic() >= deadline
                    or max_attempts is not None
                    and attempts >= max_attempts
                ):
                    raise
                retry_after = getattr(exc, "retry_after", None)
                response = getattr(exc, "response", None)
                if response is not None:
                    retry_after = response.headers.get("Retry-After", retry_after)
                wait = delay
                if retry_after:
                    try:
                        wait = max(wait, float(retry_after))
                    except ValueError:
                        wait = max(
                            wait,
                            parsedate_to_datetime(retry_after).timestamp()
                            - time.time(),
                        )
                self.e.log(
                    "control_retry",
                    error_type=type(exc).__name__,
                    status=http_status(exc),
                    wait_s=wait,
                )
                if http_status(exc) == 429:
                    self.next_control_request = max(
                        self.next_control_request, time.monotonic() + wait
                    )
                await asyncio.sleep(min(wait, max(0, deadline - time.monotonic())))
                delay = min(4, delay * 2)

    async def control_guest(self, sid, payload, deadline, *, retry=True):
        async def call(timeout):
            sent = time.monotonic()
            value = await self.guest(sid, payload, timeout)
            return value, sent, time.monotonic()

        return await self.control(call, deadline, retry=retry)

    async def observe_controller(self):
        while True:
            scheduled = time.monotonic() + 0.1
            await asyncio.sleep(0.1)
            received = time.monotonic()
            self.loop_observations.append(
                {
                    "scheduled": scheduled,
                    "received": received,
                    "delay_s": max(0, received - scheduled),
                }
            )

    async def exec(self, sid, command, timeout=180):
        start = time.monotonic()
        try:
            r = await self.p.exec(sid, command, timeout)
            received = time.monotonic()
            self.e.log(
                "command",
                id=sid,
                command_sha256=hashlib.sha256(command.encode()).hexdigest(),
                duration_s=received - start,
                sent_monotonic=start,
                received_monotonic=received,
                exit_code=r.exit_code,
                stdout=r.stdout[-2000:],
                stderr=r.stderr[-2000:],
                observation=r.observation,
            )
            if r.exit_code != 0:
                raise ProviderError(
                    f"Guest command exited {r.exit_code}: {r.stdout[-1000:]} {r.stderr[-1000:]}"
                )
            return r.stdout
        except Exception as exc:
            self.e.log(
                "command_error",
                id=sid,
                duration_s=time.monotonic() - start,
                error=str(exc),
                error_type=type(exc).__name__,
                status=http_status(exc),
            )
            raise

    async def terminal(self, sid, row):
        deadline, delay = row["t0"] + 180, 0.1
        row["readiness_observations"] = []
        while time.monotonic() < deadline:
            sent = time.monotonic()
            try:
                output = await self.exec(
                    sid,
                    python(
                        "import time,json;print(json.dumps({'monotonic':time.monotonic()}))"
                    ),
                    min(20, deadline - time.monotonic()),
                )
                received = time.monotonic()
                data = json.loads(output)
                row["terminal_ready_s"] = time.monotonic() - row["t0"]
                row["anchor"] = {
                    "guest": data["monotonic"],
                    "controller": received,
                    "controller_before": sent,
                }
                row["readiness_observations"].append(
                    {
                        "sent_s": sent - row["t0"],
                        "received_s": received - row["t0"],
                        "passed": True,
                    }
                )
                return
            except Exception as exc:
                row["readiness_observations"].append(
                    {
                        "sent_s": sent - row["t0"],
                        "received_s": time.monotonic() - row["t0"],
                        "passed": False,
                    }
                )
                row.setdefault("readiness_errors", []).append(
                    {"at": now(), "error": str(exc), "delay_s": delay}
                )
                if (
                    getattr(exc, "terminal", False)
                    or http_status(exc) in (401, 403, 429)
                    or isinstance(exc, json.JSONDecodeError)
                ):
                    raise
                await asyncio.sleep(min(delay, max(0, deadline - time.monotonic())))
        raise TimeoutError("Terminal readiness exceeded 180 seconds")

    async def bootstrap(self, sid, row):
        start = time.monotonic()
        # Package installation is part of cold workflow time, reported separately.
        # Users can supply a tools image; this check never silently treats it as warm.
        script = INSTALL
        await self.exec(sid, script, 1200)
        encoded = base64.b64encode(self.tools_archive()).decode()
        await self.exec(
            sid,
            "printf %s " + shlex.quote(encoded) + " | base64 -d | "
            "$(command -v sudo || true) tar xzf - -C /opt/bench",
            120,
        )
        row["bootstrap_s"] = time.monotonic() - start

    def tools_archive(self):
        cfg = json.loads((assets() / "manifest.example.json").read_text())
        cfg.update(
            provider=self.c["provider"],
            bucket=self.s3.config["bucket"],
            region=self.s3.config["region"],
            seed_relative_path=self.s3.config["seed"],
            seed_sha256=self.s3.seed_hash,
            s3_prefixes={self.c["provider"]: self.s3.config["prefix"]},
        )
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as archive:
            for name in (
                "sandbox_workload.py",
                "run_sandbox_workload.py",
                "record_resource_usage.py",
                "shared-app.bundle",
            ):
                archive.add(assets() / name, arcname=name)
            data = json.dumps(cfg).encode()
            info = tarfile.TarInfo("manifest.json")
            info.size, info.mode = len(data), 0o600
            archive.addfile(info, io.BytesIO(data))
            checksums = "".join(
                hashlib.sha256((assets() / name).read_bytes()).hexdigest()
                + "  "
                + name
                + "\n"
                for name in (
                    "sandbox_workload.py",
                    "run_sandbox_workload.py",
                    "record_resource_usage.py",
                    "shared-app.bundle",
                )
            ).encode()
            info = tarfile.TarInfo("prepared.sha256")
            info.size = len(checksums)
            archive.addfile(info, io.BytesIO(checksums))
        return buf.getvalue()

    async def prepare_tools(self):
        started = time.monotonic()
        name = "sb-" + self.run_id + "-tools"
        row = {
            "case": "source-tools",
            "index": 0,
            "status": "dispatched",
            "preparation": True,
            "t0": started,
        }
        self.e.state["trials"].append(row)
        self.e.save()
        try:
            await self.admit(1)
            if self.p.native_tools_build:
                ref = {"id": name, "image_name": name, "kind": "image", "role": "tools"}
                self.e.state["snapshots"].append(ref)
                self.e.save()
                sid = await self.e.mutation(
                    "create",
                    lambda: self.p.build_tools(name, INSTALL, self.tools_archive()),
                    name=name,
                )
            else:
                if self.c["provider"] == "boat":
                    self.e.state["snapshots"].append(
                        {"id": name, "kind": "tools_image", "role": "tools"}
                    )
                    self.e.save()
                sid = await self.e.mutation(
                    "create", lambda: self.p.create(name), name=name
                )
            self.e.state["owned"][sid] = {"name": name, "deleted": False}
            row["sandbox_id"] = sid
            # Image builds have their own deadline; readiness starts after deployment.
            row["t0"] = time.monotonic()
            await self.terminal(sid, row)
            if not self.p.native_tools_build:
                await self.bootstrap(sid, row)
            ref = await self.e.mutation(
                "snapshot", lambda: self.p.tools_snapshot(sid, name), id=sid, name=name
            )
            ref.update(source_id=sid, role="tools")
            if self.p.native_tools_build:
                self.e.state["snapshots"][0] = ref
                self.c["image"] = ref["id"]
            elif self.c["provider"] in ("boat", "modal"):
                self.c["image"] = ref["id"]
                if self.c["provider"] == "boat":
                    self.e.state["snapshots"][0] = ref
                else:
                    self.e.state["snapshots"].append(ref)
            else:
                self.c["prepared_ref"] = ref
                self.e.state["snapshots"].append(ref)
            if self.p.release_snapshot_source:
                await self.cleanup_ids([sid])
                if not self.e.state["owned"][sid].get("deleted"):
                    raise RuntimeError("Tools source deletion not confirmed; dispatch held")
            row["status"] = "passed"
            self.e.state["prepared_tools"] = {
                "reference": ref,
                "duration_s": time.monotonic() - started,
                "creation_method": "disk clone of stopped tools source"
                if self.c["provider"] == "exe"
                else "prepared base image",
                "cold_install_in_measured_cases": False,
            }
        except BaseException as exc:
            row.update(status="failed", error=str(exc))
            raise
        finally:
            row["duration_s"] = time.monotonic() - started
            self.e.save()

    async def start_guest(self, sid):
        env = {**self.s3.env, "BENCH_BIND_HOST": "127.0.0.1"}
        command = "env " + " ".join(shlex.quote(k + "=" + v) for k, v in env.items())
        command += " /opt/bench-venv/bin/python -u /opt/bench/run_sandbox_workload.py"
        for service, service_command, log_path in [
            ("guest", command, "/var/log/bench/guest.log"),
            (
                "recorder",
                "python3 /opt/bench/record_resource_usage.py",
                "/var/log/bench/recorder.log",
            ),
        ]:
            pid = await self.p.start_service(sid, service_command, log_path)
            self.e.log("service_started", id=sid, service=service, pid=pid)
        for attempt in range(30):
            try:
                await self.guest(sid, {"op": "progress"}, 10)
                return
            except Exception:
                await asyncio.sleep(0.5)
        raise TimeoutError("Guest service did not start")

    async def guest(self, sid, payload, timeout=600, timing=None):
        # A wedged FUSE read can delay Python's signal handler in the guest.
        # Bound the observing client as well, without restarting the saved mount.
        if payload.get("op") == "state":
            timeout = min(timeout, 45)
        # A native exec reaches a loopback HTTP service. No public guest endpoint,
        # provider key, or provider admin credentials are installed in the VM.
        code = (
            "import urllib.request,json,base64;req=urllib.request.Request('http://127.0.0.1:8123/',data="
            + repr(json.dumps(payload).encode())
            + ",headers={'Content-Type':'application/json'});print(base64.b64encode(urllib.request.urlopen(req,timeout="
            + str(timeout)
            + ").read()).decode())"
        )
        # Some native exec APIs insert newlines between output chunks. Base64
        # tolerates that framing without changing JSON strings or their bytes.
        started = time.monotonic()
        output = await self.exec(sid, python(code), timeout + 10)
        data = json.loads(base64.b64decode(output))
        if not data["ok"]:
            raise GuestFailure(data)
        if timing is not None:
            elapsed = time.monotonic() - started
            timing.update(
                workflow_rpc_s=elapsed,
                guest_workflow_s=data["guest_timing"]["duration_s"],
                workflow_observation_overhead_s=max(
                    0, elapsed - data["guest_timing"]["duration_s"]
                ),
            )
            timing["guest_timing"] = data["guest_timing"]
        return data["result"]

    async def workload(self, sid, row, mode, expected=None, disk_expected=None):
        setup_started = time.monotonic()
        if expected is not None:
            observed = (await self.guest(sid, {"op": "state"}))["state"]
            row["preservation"] = {
                "passed": observed == expected,
                "expected": expected,
                "observed": observed,
            }
            if observed != expected:
                raise RuntimeError(
                    "Saved RAM/browser/app/disk state differs; refusing repair"
                )
        elif mode in ("disk", "disk_resume"):
            check = json.loads(
                await self.exec(
                    sid,
                    python(
                        "import json,hashlib;from pathlib import Path;root=Path("
                        + repr(disk_expected["repo_path"])
                        + ");"
                        "print(json.dumps({'hash':hashlib.sha256((root/'api/index.mjs').read_bytes()).hexdigest(),"
                        "'dependency':(root/'node_modules/escape-html/package.json').exists()}))"
                    ),
                )
            )
            row["disk_preservation"] = {
                "passed": check["hash"] == disk_expected["edit_sha256"]
                and check["dependency"],
                "observed": check,
            }
            if not row["disk_preservation"]["passed"]:
                raise RuntimeError("Saved disk edit or installed dependency missing")
            # RAM is deliberately not expected in a disk fork.
            await self.start_guest(sid)
        else:
            if self.c.get("prepared"):
                await self.exec(
                    sid, "cd /opt/bench && sha256sum -c prepared.sha256", 30
                )
            else:
                await self.bootstrap(sid, row)
            await self.start_guest(sid)
        row["harness_setup_s"] = time.monotonic() - setup_started
        payload = {
            "op": "setup",
            "case": row["case"],
            "index": row["index"],
            "run": self.run_id,
            "marker": self.run_id + "-" + row["case"] + "-" + str(row["index"]),
            "mode": mode,
        }
        if mode == "disk_resume":
            payload["previous_marker"] = disk_expected["marker"]
            payload["repo_path"] = disk_expected["repo_path"]
        result = await self.guest(sid, payload, 1200, timing=row)
        row["steps"] = result["steps"]
        for step in result["steps"]:
            row[step["step"] + "_ready_s"] = (
                step["guest_monotonic"]
                - row["anchor"]["guest"]
                + row["anchor"]["controller"]
                - row["t0"]
            )
        row["guest_milestones_s"] = {
            s["step"]: s["guest_monotonic"] - row["guest_timing"]["started"]
            for s in result["steps"]
        }
        row["milestone_observation_bounds_s"] = {
            s["step"]: {
                "earliest": s["guest_monotonic"]
                - row["anchor"]["guest"]
                + row["anchor"]["controller_before"]
                - row["t0"],
                "latest": row[s["step"] + "_ready_s"],
            }
            for s in result["steps"]
        }
        verification_started = time.monotonic()
        row["external_s3"] = await self.s3.verify(
            next(s["result"] for s in result["steps"] if s["step"] == "s3_write")
        )
        row["external_verification_s"] = time.monotonic() - verification_started
        if not row["external_s3"]["passed"]:
            row["failure_domain"] = "external_s3_verification"
            raise RuntimeError("Independent S3 readback did not match")
        row["full_workflow_s"] = time.monotonic() - row["t0"]
        row["s3_verified_s"] = row["full_workflow_s"]
        return result["state"]

    async def trial(
        self, case, index, ref=None, expected=None, existing=None, disk_expected=None
    ):
        row = {"case": case, "index": index, "status": "queued", "queued_at": now()}
        if case == "fresh":
            row["source_kind"] = "image"
            if self.c.get("prepared") and self.c["suite"] == "workflow":
                row["source_kind"] = (
                    "prepared_snapshot"
                    if self.c.get("prepared_ref")
                    else "prepared_image"
                )
            row["case_label"] = "Fresh (" + row["source_kind"].replace("_", " ") + ")"
        queued = time.monotonic()
        self.e.state["trials"].append(row)
        self.e.save_trial(row)
        async with self.slots:
            admitted = time.monotonic()
            row["admission_wait_s"] = admitted - queued
            # Keep source names within Beam's 24-character limit, including N=1,000.
            label = {"source-disk": "sd", "source-memory": "sm"}.get(case, case[0])
            name = f"sb-{self.run_id}-{label}-{index}"
            row.update(
                name=name, status="dispatched", t0=time.monotonic(), dispatched_at=now()
            )
            self.e.save_trial(row)
            try:

                async def dispatch():
                    row["t0"] = time.monotonic()
                    row["dispatch_preparation_s"] = row["t0"] - admitted
                    try:
                        return await (
                            self.p.resume(existing)
                            if existing
                            else self.p.create(name, ref)
                        )
                    finally:
                        received = time.monotonic()
                        row["provider_lifecycle_s"] = received - row["t0"]
                        row["lifecycle_observation"] = {
                            "sent_monotonic": row["t0"],
                            "received_monotonic": received,
                        }

                if existing:
                    sid = await self.e.mutation(
                        "resume",
                        dispatch,
                        id=existing,
                        name=name,
                    )
                    if sid != existing:
                        raise RuntimeError("Same-ID resume returned another sandbox")
                else:
                    sid = await self.e.mutation("create", dispatch, name=name)
                    self.e.state["owned"][sid] = {"name": name, "deleted": False}
                    if ref and sid == ref["source_id"]:
                        raise RuntimeError("Fork reused its source ID")
                row["sandbox_id"] = sid
                self.e.save_trial(row)
                row["post_lifecycle_recording_s"] = (
                    time.monotonic() - row["lifecycle_observation"]["received_monotonic"]
                )
                await self.terminal(sid, row)
                if self.c["suite"] == "commands":
                    # Deterministic shell/filesystem checks, separate from the
                    # full browser/S3 workload and historical command benchmark.
                    code = 'set -eu; d=$(mktemp -d); trap \'rm -rf "$d"\' EXIT; printf \'%s\' sandbox-bench > "$d/a"; cp "$d/a" "$d/b"; cmp "$d/a" "$d/b"; mv "$d/b" "$d/c"; test -s "$d/c"; uname -s; printf \'\\nCOMMANDS_OK\\n\''
                    out = await self.exec(sid, code)
                    if not out.endswith("COMMANDS_OK\n"):
                        raise RuntimeError("Command verification marker missing")
                    row["commands_s"] = time.monotonic() - row["t0"]
                else:
                    mode = (
                        ("resume" if expected else "disk_resume")
                        if existing
                        else (
                            ref["kind"]
                            if ref
                            else "source"
                            if case.startswith("source-")
                            else "fresh"
                        )
                    )
                    row["saved_state"] = await self.workload(
                        sid,
                        row,
                        mode,
                        expected,
                        disk_expected or (ref or {}).get("disk_expected"),
                    )
                row["status"] = "passed"
            except asyncio.CancelledError:
                row.update(
                    status="interrupted", error="Controller interrupted this trial"
                )
                raise
            except Exception as exc:
                row.update(status="failed", error=type(exc).__name__ + ": " + str(exc))
                if isinstance(exc, GuestFailure):
                    row["guest_failure"] = exc.result
                    row["steps"] = exc.result.get("steps", [])
                    row["failure_domain"] = "guest_workload"
                if getattr(exc, "domain", None):
                    row["failure_domain"] = exc.domain
                row.setdefault("failure_domain", "unknown")
            finally:
                row["duration_s"] = time.monotonic() - row["t0"]
                self.e.save_trial(row)
            return row

    async def activity(self, rows):
        if self.c["suite"] != "workflow":
            return
        ready = [r for r in rows if r["status"] == "passed"]
        alignments = {}

        async def calibrate(row):
            try:
                value, sent, received = await self.control_guest(
                    row["sandbox_id"], {"op": "progress"}, time.monotonic() + 180
                )
                alignment = ClockAlignment.sample(
                    sent, received, value["guest_monotonic"]
                )
                if alignment.uncertainty_s > 1:
                    raise RuntimeError(
                        "Activity clock uncertainty exceeds the one-second cadence tolerance"
                    )
                alignments[row["sandbox_id"]] = alignment
                row["activity_clock"] = {
                    "controller_sent": sent,
                    "controller_received": received,
                    "guest_monotonic": value["guest_monotonic"],
                    "offset_lower_s": alignment.lower,
                    "offset_upper_s": alignment.upper,
                }
            except Exception as exc:
                row["activity"] = {
                    "passed": False,
                    "armed": False,
                    "observed": False,
                    "error": type(exc).__name__ + ": " + str(exc),
                    "failure_domain": "harness_control",
                }

        await asyncio.gather(*(calibrate(r) for r in ready))
        ready = [r for r in ready if r["sandbox_id"] in alignments]
        if not ready:
            self.e.save()
            return
        max_rtt = max(2 * a.uncertainty_s for a in alignments.values())
        lead = max(
            5,
            len(ready) / self.p.control_requests_per_second
            + math.ceil(len(ready) / self.p.control_concurrency) * max_rtt
            + 2,
        )
        start = time.monotonic() + lead
        epoch = time.time() + lead

        async def schedule(row):
            sid = row["sandbox_id"]
            alignment = alignments[sid]
            row["activity_schedule"] = {
                "lead_s": lead,
                "epoch": epoch,
                "start_monotonic": start,
                "guest_start_monotonic": alignment.guest_at(start),
                "alignment_confirmed": False,
            }
            try:
                receipt, sent, received = await self.control_guest(
                    sid,
                    {
                        "op": "activity",
                        "epoch": epoch,
                        "start_monotonic": alignment.guest_at(start),
                        "cycles": 12,
                        "duration": 60,
                    },
                    start + 30,
                    retry=False,
                )
                alignment = alignment.intersect(
                    ClockAlignment.sample(sent, received, receipt["guest_monotonic"])
                )
                alignments[sid] = alignment
                row["activity_schedule"].update(
                    armed_at=receipt["scheduled_at"],
                    arm_received_monotonic=received,
                    arm_rtt_s=received - sent,
                    alignment_uncertainty_s=alignment.uncertainty_s,
                    alignment_confirmed=received <= start
                    and alignment.uncertainty_s <= 1,
                )
            except Exception as exc:
                # An arm is stateful. Its outcome is unknown on transport failure;
                # never submit it again. A read may still recover guest evidence.
                row["activity_schedule"]["arm_error"] = (
                    type(exc).__name__ + ": " + str(exc)
                )

        await asyncio.gather(*(schedule(r) for r in ready))
        await asyncio.sleep(max(0, start + 60 - time.monotonic()))
        deadline = time.monotonic() + 180

        async def collect(row):
            value = None
            error = None
            drain_started = time.monotonic()
            try:
                delay = 0.5
                async with asyncio.timeout_at(deadline):
                    while True:
                        result, sent, received = await self.control_guest(
                            row["sandbox_id"], {"op": "progress"}, deadline
                        )
                        value = result["activity"]
                        if value is None:
                            break
                        # Confirm that the monotonic clock has not reset since arm.
                        alignments[row["sandbox_id"]] = alignments[
                            row["sandbox_id"]
                        ].intersect(
                            ClockAlignment.sample(
                                sent, received, result["guest_monotonic"]
                            )
                        )
                        if value["done"]:
                            break
                        await asyncio.sleep(
                            min(delay, max(0, deadline - time.monotonic()))
                        )
                        delay = min(4, delay * 2)
                    if value is not None:
                        for event in value["events"]:
                            if event.get("s3_write"):
                                event["external_s3"] = await self.s3.verify(
                                    event["s3_write"]
                                )
            except Exception as exc:
                error = type(exc).__name__ + ": " + str(exc)
            schedule = row["activity_schedule"]
            if value is None:
                row["activity"] = {
                    "passed": False,
                    "armed": False if error is None else None,
                    "observed": error is None,
                    "events": [],
                    "done": False,
                }
            else:
                # A failed read or inconsistent clock invalidates scoring, while
                # preserving every event recovered before the error.
                schedule["alignment_confirmed"] &= error is None
                row["activity"] = score_activity(
                    value, schedule, alignments[row["sandbox_id"]]
                )
                if error is not None:
                    row["activity"]["observed"] = False
            row["activity"]["drain_wait_s"] = time.monotonic() - drain_started
            if error or schedule.get("arm_error"):
                row["activity"].update(
                    error=error or schedule["arm_error"],
                    failure_domain="harness_control",
                )
            self.e.save_trial(row)

        await asyncio.gather(*(collect(r) for r in ready))
        self.e.save()

    async def artifact(self, row, deadline=None):
        if (
            self.c["suite"] != "workflow"
            or not row.get("sandbox_id")
            or row.get("artifact")
        ):
            return
        deadline = min(deadline or float("inf"), time.monotonic() + 120)
        code = (
            "import io,zipfile,base64;from pathlib import Path;b=io.BytesIO();z=zipfile.ZipFile(b,'w',zipfile.ZIP_DEFLATED);"
            "[(z.write(p,str(Path(base.name)/p.relative_to(base)))) for base in [Path('/workspace/bench-evidence'),Path('/var/log/bench')] for p in base.rglob('*') if p.is_file()];z.close();print(base64.b64encode(b.getvalue()).decode())"
        )
        try:
            data = await self.control(
                lambda timeout: self.exec(
                    row["sandbox_id"],
                    "$(command -v sudo || true) " + python(code),
                    timeout,
                ),
                deadline,
                call_timeout=120,
                max_attempts=3,
            )
            path = self.e.path / "artifacts" / f"{row['case']}-{row['index']}.zip"
            path.parent.mkdir(exist_ok=True)
            path.write_bytes(base64.b64decode("".join(data.split()), validate=True))
            row["artifact"] = str(path.relative_to(self.e.path))
            row.pop("collection_error", None)
            row.pop("archive_missing_at_delete", None)
        except Exception as exc:
            row["collection_error"] = type(exc).__name__ + ": " + str(exc)

        finally:
            if not row.get("artifact"):
                row["archive_missing_at_delete"] = True
            self.e.save_trial(row)

    async def cleanup_ids(self, ids, confirm_delete=()):
        owned = sorted(set(ids))
        for start in range(0, len(owned), self.p.cleanup_concurrency):
            chunk = owned[start : start + self.p.cleanup_concurrency]
            deadline = time.monotonic() + (self.p.cleanup_timeout or 90)
            started = time.monotonic()
            pending = set(chunk)

            def mark_absent(absent):
                for sid in absent:
                    pending.discard(sid)
                    record = self.e.state["owned"][sid]
                    record.update(deleted=True, verified_at=now())
                    record.pop("cleanup_error", None)
                    for op in self.e.state["operations"]:
                        if (
                            op["kind"] == "delete"
                            and op.get("id") == sid
                            and op["status"] == "unconfirmed"
                        ):
                            op["reconciled_absent_at"] = record["verified_at"]
                            self.e.save_update("operation", op["operation_id"], op)

            try:
                async with asyncio.timeout_at(deadline):
                    await asyncio.gather(*(self.p.release(sid) for sid in chunk))
                    mark_absent(
                        await self.control(
                            lambda _: self.p.absent_many(chunk), deadline
                        )
                    )

                    async def remove(sid):
                        earlier = [
                            o
                            for o in self.e.state["operations"]
                            if o["kind"] == "delete"
                            and o.get("id") == sid
                            and o["status"] == "unconfirmed"
                            and not o.get("reconciled_absent_at")
                        ]
                        if earlier and sid not in confirm_delete:
                            self.e.state["owned"][sid]["cleanup_error"] = (
                                "Earlier deletion outcome is unconfirmed; not replaying it"
                            )
                            return
                        missing = [
                            r
                            for r in self.e.state["trials"]
                            if r.get("sandbox_id") == sid
                            and r["status"] == "passed"
                            and self.c["suite"] == "workflow"
                            and not r.get("artifact")
                        ]
                        for row in missing:
                            row["archive_missing_at_delete"] = True
                            self.e.save_trial(row)
                        try:
                            await self.control(
                                lambda _: self.e.mutation(
                                    "delete",
                                    lambda: self.p.delete(sid),
                                    id=sid,
                                    operator_confirmed=sid in confirm_delete,
                                    prior_operation_ids=[
                                        o["operation_id"] for o in earlier
                                    ],
                                ),
                                deadline,
                                retry=False,
                            )
                        except Exception as exc:
                            self.e.state["owned"][sid]["cleanup_error"] = (
                                type(exc).__name__ + ": " + str(exc)
                            )

                    await asyncio.gather(*(remove(sid) for sid in list(pending)))
                    delay = 1
                    while pending and time.monotonic() < deadline:
                        mark_absent(
                            await self.control(
                                lambda _: self.p.absent_many(sorted(pending)), deadline
                            )
                        )
                        if pending:
                            await asyncio.sleep(
                                min(delay, max(0, deadline - time.monotonic()))
                            )
                            delay = min(8, delay * 2)
                    if pending:
                        raise RuntimeError(
                            "Deletion was not independently confirmed before the deadline"
                        )
            except Exception as exc:
                for sid in pending:
                    self.e.state["owned"][sid]["cleanup_error"] = (
                        type(exc).__name__
                        + ": "
                        + (
                            str(exc)
                            or "Deletion was not independently confirmed before the deadline"
                        )
                    )
            finally:
                for sid in chunk:
                    record = self.e.state["owned"][sid]
                    self.e.log(
                        "cleanup_vm",
                        id=sid,
                        duration_s=time.monotonic() - started,
                        deleted=record.get("deleted"),
                        error=record.get("cleanup_error"),
                    )
                self.e.save()

    async def cleanup(self, confirm_delete=()):
        # Reconcile names after an ambiguous create before deleting known IDs.
        try:
            inventory = await self.control(
                lambda _: self.p.inventory(), time.monotonic() + 90
            )
            intents = {
                o.get("name")
                for o in self.e.state["operations"]
                if o["kind"] == "create"
            }
            for vm in inventory:
                if vm.get("name") in intents or vm.get("owned_run") == self.run_id:
                    self.e.state["owned"].setdefault(
                        vm["id"], {"name": vm["name"], "deleted": False}
                    )
            self.e.state.pop("reconciliation_error", None)
        except Exception as exc:
            self.e.state["reconciliation_error"] = type(exc).__name__ + ": " + str(exc)
        await self.cleanup_ids(list(self.e.state["owned"]), confirm_delete)
        for ref in self.e.state["snapshots"]:
            if ref.get("cleanup_policy") == "provider_expiry" or (
                self.c["provider"] == "modal" and ref["kind"] == "memory"
            ):
                ref.update(status="provider_expiry_pending", deleted=False)
                self.e.save()
                continue
            try:
                deadline = time.monotonic() + 60
                if not await self.control(
                    lambda _: self.p.snapshot_absent(ref), deadline
                ):
                    if not ref.get("delete_submitted"):
                        if any(
                            o["kind"] == "delete_snapshot"
                            and o.get("snapshot", {}).get("id") == ref["id"]
                            and o["status"] == "unconfirmed"
                            for o in self.e.state["operations"]
                        ):
                            raise RuntimeError(
                                "Earlier snapshot deletion is unconfirmed; not replaying it"
                            )
                        await self.control(
                            lambda _: self.e.mutation(
                                "delete_snapshot",
                                lambda: self.p.delete_snapshot(ref),
                                snapshot=ref,
                            ),
                            deadline,
                            retry=False,
                        )
                        ref["delete_submitted"] = True
                    deadline = min(deadline, time.monotonic() + 30)
                    delay = 1
                    while True:
                        ref["deleted"] = await self.control(
                            lambda _: self.p.snapshot_absent(ref), deadline, retry=False
                        )
                        if ref["deleted"]:
                            break
                        await asyncio.sleep(
                            min(delay, max(0, deadline - time.monotonic()))
                        )
                        delay = min(16, delay * 2)
                else:
                    ref["deleted"] = True
                if ref.get("deleted"):
                    ref.pop("cleanup_error", None)
                    for op in self.e.state["operations"]:
                        if op["kind"] == "delete_snapshot" and op.get("snapshot", {}).get("id") == ref["id"] and op["status"] == "unconfirmed":
                            op["reconciled_absent_at"] = now()
                            self.e.save_update("operation", op["operation_id"], op)
            except Exception as exc:
                ref["cleanup_error"] = type(exc).__name__ + ": " + str(exc)
            self.e.save()
        if self.s3:
            if (
                self.e.state.get("reconciliation_error")
                or any(not r.get("deleted") for r in self.e.state["owned"].values())
                or any(
                    o["kind"] == "create" and o["status"] == "unconfirmed"
                    for o in self.e.state["operations"]
                )
            ):
                self.e.state["s3_cleanup"] = {
                    "deferred": "Run-owned writers are not confirmed absent"
                }
            else:
                try:
                    self.e.state["s3_cleanup"] = await self.s3.cleanup(self.run_id)
                except Exception as exc:
                    self.e.state["s3_cleanup"] = {"error": str(exc)}
        self.e.save()

    async def finish(self):
        try:
            deadline = time.monotonic() + 120
            async with asyncio.timeout_at(deadline):
                await asyncio.gather(
                    *(
                        self.artifact(r, deadline)
                        for r in self.e.state["trials"]
                        if r["status"] == "passed"
                        and r.get("sandbox_id")
                        and not self.e.state["owned"][r["sandbox_id"]].get("deleted")
                    )
                )
        except TimeoutError:
            self.e.log("collection_budget_exhausted", budget_s=120)
        finally:
            await self.cleanup()

    def require_confirmed_operations(self):
        if any(
            o["status"] == "unconfirmed" and not o.get("reconciled_absent_at")
            for o in self.e.state["operations"]
        ):
            raise RuntimeError("Unconfirmed lifecycle operation; no further dispatch")

    async def admit(self, additional):
        capacity = await self.p.check_capacity(additional)
        if capacity is not None:
            self.e.log("capacity_admission", **capacity)
            if not capacity["admitted"]:
                raise RuntimeError("Insufficient organization capacity for this batch; dispatch held")

    async def resume_trial(self, row):
        try:
            if row["status"] != "passed":
                raise RuntimeError("Fresh workload did not pass")
            sid = row["sandbox_id"]
            async with self.slots:
                preserved = (
                    (await self.guest(sid, {"op": "state"}))["state"]
                    if self.p.resume_memory
                    else None
                )
                await self.e.mutation("pause", lambda: self.p.pause(sid), id=sid)
            return await self.trial(
                "resume",
                row["index"],
                expected=preserved,
                existing=sid,
                disk_expected=row["saved_state"],
            )
        except Exception as exc:
            result = {
                "case": "resume",
                "index": row["index"],
                "status": "ineligible",
                "error": str(exc),
            }
            self.e.state["trials"].append(result)
            self.e.save_trial(result)
            return None

    async def phase(self, case):
        self.require_confirmed_operations()
        if self.s3:
            self.s3.ensure_valid()
        ref, expected = None, None
        source_ids = []
        if case in ("disk", "memory"):
            await self.admit(1)
            source = await self.trial("source-" + case, 0)
            if source["status"] != "passed":
                raise RuntimeError("Snapshot source workload failed")
            source["preparation"] = True
            sid = source["sandbox_id"]
            source_ids.append(sid)
            await self.exec(
                sid,
                python(
                    "from pathlib import Path;Path('/workspace/source-disk-marker').write_text("
                    + repr(self.run_id)
                    + ")"
                ),
            )
            expected = (
                (await self.guest(sid, {"op": "state"}))["state"]
                if case == "memory"
                else None
            )
            ref = await self.e.mutation(
                "snapshot",
                lambda: self.p.snapshot(sid, case, self.run_id + "-" + case),
                id=sid,
            )
            ref.update(source_id=sid, disk_expected=source["saved_state"])
            self.e.state["snapshots"].append(ref)
            self.e.save()
            if self.p.release_snapshot_source:
                await self.cleanup_ids(source_ids)
                if not self.e.state["owned"][sid].get("deleted"):
                    raise RuntimeError("Snapshot source deletion not confirmed; dispatch held")
        for start in range(1, self.c["count"] + 1, self.c["batch_size"]):
            if self.s3:
                self.s3.ensure_valid()
            self.require_confirmed_operations()
            indices = range(
                start, min(start + self.c["batch_size"], self.c["count"] + 1)
            )
            await self.admit(len(indices))
            rows = await asyncio.gather(
                *(self.trial(case, i, ref, expected) for i in indices)
            )
            await self.activity(rows)
            await asyncio.gather(*(self.artifact(r) for r in rows))
            capacity_failed = any(
                r.get("failure_domain") == "provider_capacity" for r in rows
            )
            if (
                case == "fresh"
                and "resume" in self.c["lifecycles"]
                and not capacity_failed
            ):
                resumed = await asyncio.gather(
                    *(self.resume_trial(row) for row in rows)
                )
                resumed = [row for row in resumed if row is not None]
                await self.activity(resumed)
                await asyncio.gather(*(self.artifact(r) for r in resumed))
            await self.cleanup_ids(
                [r["sandbox_id"] for r in rows if r.get("sandbox_id")]
            )
            if capacity_failed:
                raise RuntimeError(
                    "Provider capacity rejected this batch; later lifecycle cases remain unrun"
                )
            if any(
                not v.get("deleted")
                for k, v in self.e.state["owned"].items()
                if k not in source_ids
                and k
                != self.e.state.get("prepared_tools", {})
                .get("reference", {})
                .get("source_id")
            ):
                raise RuntimeError("Cleanup not confirmed; next batch held")
        await self.cleanup_ids(source_ids)

    async def run(self):
        self.e.state["status"] = "running"
        self.e.state["timing_contract"] = {
            "provider_lifecycle_s": "Native create/fork/resume call, including network and any SDK-internal waits; excludes tools preparation and controller journal writes.",
            "terminal_ready_s": "Observed first successful command since native dispatch; readiness probes retry at 100 ms after failure. Attempts and RPC bounds are retained.",
            "guest_workflow_s": "Monotonic duration on the guest workload thread; excludes SDK transport, harness deployment and outside S3 verification.",
            "harness_setup_s": "Runtime service startup, prepared image checks, cold bootstrap when requested, and saved-state observation before workload execution.",
            "full_workflow_s": "Observed end-to-end from native dispatch through independent S3 verification, including harness setup. Never adjusted by subtracting estimated overhead.",
            "s3_write_ready_s": "Guest S3 write completion, translated to the latest edge of the terminal-probe clock-alignment interval; excludes subsequent state capture and independent verification.",
            "s3_verified_s": "Controller observation after independent S3 readback succeeds; currently the final full-workflow milestone.",
            "controller_observations": "Direct event-loop lateness samples at 100 ms, local evidence-write intervals, command/lifecycle send/receive times and S3 executor queue/return delays. No observed delay is subtracted from workload timings.",
            "activity": "One shared future controller-monotonic target mapped through fresh guest clock intervals. Conservative event bounds must meet the unchanged 5-second schedule, 1-second lateness tolerance and 61-second completion limit. Unconfirmed alignment is never a pass. Wall clocks are diagnostic only.",
            "control_requests_per_second": self.p.control_requests_per_second,
            "native_waits": {
                "beam": "Native wait=True first; when it returns a running PID, observe that same PID at 100 ms without replay; each observation is logged",
                "tensorlake": "Blocking create; resume SDK polling at 100 ms",
                "modal": "Native stream reads and process wait",
                "boat": "Blocking command endpoint",
                "blaxel": "Native wait_for_completion=True",
                "exe": "Blocking NDJSON command response",
            }[self.c["provider"]],
        }
        self.e.save()
        monitor = asyncio.create_task(self.observe_controller())
        try:
            if self.c.get("prepared") and self.c["suite"] == "workflow":
                await self.prepare_tools()
            for case in self.c["lifecycles"]:
                if case != "resume":
                    await self.phase(case)
            self.e.state["status"] = "finished"
        except BaseException as exc:
            self.e.state.update(
                status="interrupted", error=type(exc).__name__ + ": " + str(exc)
            )
            raise
        finally:
            try:
                finish = asyncio.create_task(self.finish())
                # Finalization has bounded collection and per-chunk cleanup budgets.
                # Repeated SIGINT must not cancel deletion halfway through.
                while not finish.done():
                    try:
                        await asyncio.shield(finish)
                    except asyncio.CancelledError:
                        continue
                await finish
            finally:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass
                self.e.state["finished_at"] = now()
                observations = {
                    "sample_interval_s": 0.1,
                    "event_loop": self.loop_observations,
                    "evidence_io": list(self.e.io_observations),
                }
                self.e.state["controller_observations"] = {
                    "path": "controller-observations.json",
                    "loop_delay_s": timing_stats(
                        [x["delay_s"] for x in self.loop_observations]
                    ),
                    "evidence_io_s": timing_stats(
                        [x["finished"] - x["started"] for x in self.e.io_observations]
                    ),
                }
                self.e.write(self.e.path / "controller-observations.json", observations)
                self.e.save()


def timing_stats(values):
    values = sorted(values)
    return {
        "n": len(values),
        "mean": statistics.mean(values) if values else None,
        "p95": values[math.ceil(len(values) * 0.95) - 1] if values else None,
        "max": values[-1] if values else None,
    }


def summarize(state):
    result = {
        "run_id": state["run_id"],
        "provider": state["config"]["provider"],
        "status": state["status"],
        "timing_protocol": state.get("timing_protocol"),
        "activity_protocol": state.get("activity_protocol"),
        "prepared_tools": state.get("prepared_tools"),
        "timing_contract": state.get("timing_contract"),
        "controller_observations": state.get("controller_observations"),
        "cases": {},
    }
    for case in state["config"]["lifecycles"]:
        rows = [r for r in state["trials"] if r["case"] == case]
        values = sorted(
            r["full_workflow_s"]
            for r in rows
            if r.get("full_workflow_s") is not None and r["status"] == "passed"
        )
        result["cases"][case] = {
            "label": rows[0].get("case_label", case) if rows else case,
            "timing_population": "successful_workflows",
            "requested": state["config"]["count"],
            "attempted": sum("t0" in r for r in rows),
            "passed": sum(r["status"] == "passed" for r in rows),
            "archives": sum(bool(r.get("artifact")) for r in rows if r["status"] == "passed"),
            "missing_archives": [r["index"] for r in rows if r["status"] == "passed" and state["config"]["suite"] == "workflow" and not r.get("artifact")],
            "ineligible": sum(r["status"] == "ineligible" for r in rows),
            "unrun": state["config"]["count"]
            - len(rows)
            + sum(r["status"] == "queued" for r in rows),
            "clean_activity": sum(
                r.get("activity", {}).get("passed", False) for r in rows
            ),
            "failed": sum(r["status"] == "failed" for r in rows),
            "interrupted": sum(r["status"] == "interrupted" for r in rows),
            "successful_workflow_s": {
                "n": len(values),
                "mean": statistics.mean(values) if values else None,
                "p95": values[max(0, math.ceil(len(values) * 0.95) - 1)]
                if values
                else None,
            },
            "errors": [
                {
                    "index": r["index"],
                    "error": r.get("error"),
                    "domain": r.get("failure_domain"),
                }
                for r in rows
                if r["status"] != "passed"
            ],
        }
        activity_rows = [r["activity"] for r in rows if r.get("activity") is not None]
        categories = [activity_category(a) for a in activity_rows]
        result["cases"][case].update(
            {
                "activity_functional": sum(
                    len(a.get("events", [])) == 12
                    and all(
                        e.get("passed") and e.get("external_s3", {}).get("passed")
                        for e in a["events"]
                    )
                    for a in activity_rows
                ),
                **{
                    "activity_" + category: categories.count(category)
                    for category in (
                        "cadence_miss",
                        "unarmed",
                        "unobserved",
                        "functional_failure",
                        "unknown",
                    )
                },
                "activity_tick_zero_s": timing_stats(
                    [
                        a["events"][0]["duration_s"]
                        for a in activity_rows
                        if a.get("events")
                    ]
                ),
            }
        )
        result["cases"][case]["timings_s"] = {}
        for field in (
            "terminal_ready_s",
            "local_write_ready_s",
            "s3_write_ready_s",
            "s3_verified_s",
            "full_workflow_s",
            "commands_s",
            "bootstrap_s",
            "provider_lifecycle_s",
            "harness_setup_s",
            "guest_workflow_s",
            "workflow_rpc_s",
            "workflow_observation_overhead_s",
            "external_verification_s",
            "dispatch_preparation_s",
            "admission_wait_s",
            "post_lifecycle_recording_s",
        ):
            measured = sorted(
                r[field]
                for r in rows
                if r["status"] == "passed" and r.get(field) is not None
            )
            result["cases"][case]["timings_s"][field] = {
                "n": len(measured),
                "mean": statistics.mean(measured) if measured else None,
                "p95": measured[max(0, math.ceil(len(measured) * 0.95) - 1)]
                if measured
                else None,
            }
    result["cleanup"] = {
        "owned": len(state["owned"]),
        "deleted": sum(x.get("deleted", False) for x in state["owned"].values()),
        "snapshots": state["snapshots"],
        "s3": state.get("s3_cleanup"),
        "unconfirmed_operations": [
            o for o in state["operations"]
            if o["status"] == "unconfirmed" and not o.get("reconciled_absent_at")
        ],
        "reconciled_operations": [
            o for o in state["operations"] if o.get("reconciled_absent_at")
        ],
    }
    return result


def cleanup_complete(report):
    clean = report["cleanup"]
    return (
        clean["owned"] == clean["deleted"]
        and not clean["unconfirmed_operations"]
        and all(x.get("deleted") for x in clean["snapshots"])
        and (
            not clean["s3"]
            or clean["s3"].get("remaining") == 0
            and not clean["s3"].get("errors")
        )
    )


def complete(report, config):
    return (
        report["status"] == "finished"
        and cleanup_complete(report)
        and all(
            v["passed"] == v["requested"]
            and (config["suite"] != "workflow" or v["clean_activity"] == v["passed"] and not v["missing_archives"])
            for v in report["cases"].values()
        )
    )
