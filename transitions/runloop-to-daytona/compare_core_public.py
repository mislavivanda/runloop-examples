"""Compare Runloop devboxes and Daytona sandboxes through their Python SDKs."""

import argparse
import asyncio
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

from daytona import (
    AsyncDaytona,
    CreateSandboxFromSnapshotParams,
    DaytonaNotFoundError,
    SandboxState,
    SessionExecuteRequest,
)
from runloop_api_client import AsyncRunloopSDK

TOKEN = uuid.uuid4().hex
NAME = f"transition-probe-{TOKEN[:12]}"
LABELS = {"transition-probe": TOKEN[:12]}
PATH = f"/tmp/transition-probe-{TOKEN}"
CONTENT = f"transition probe {TOKEN} — café\n"
SESSION = "transition-probe"
KEEP_ALIVE = {"keep_alive_time_seconds": 3600}
TTL_MINUTES = 60
CAPTURE_TIMEOUT = 1800
CURL = "curl -s -o /dev/null --connect-timeout 10 --max-time 10"
# curl exit codes for blocked traffic: DNS, connect, timeout, TLS, empty reply, receive.
DENIED = {6, 7, 28, 35, 52, 56}
BINARY = b"binary\x00fixture"
LARGE = bytes(1048576)
FIXTURE = (
    f"mkdir -p {PATH}; printf 'binary\\000fixture' > {PATH}/binary; "
    f"chmod 750 {PATH}/binary; ln -s binary {PATH}/link; "
    f"head -c 1048576 /dev/zero > {PATH}/large"
)
MANIFEST = (
    f"cd {PATH} && sha256sum binary large && stat -c '%a' binary && readlink link"
)
WORKER = f"transition-worker-{TOKEN}"
START_WORKER = (
    f"bash -c 'exec -a {WORKER} sleep 600' >/dev/null 2>&1 & echo $! > {PATH}/process"
)
WORKER_IDENTITY = f"tr '\\000' ' ' < /proc/$(cat {PATH}/process)/cmdline"

report: dict = {
    "versions": {},
    "resources": {},
    "cases": [],
    "cleanup": {},
    "errors": [],
}
cleanup: list = []
daytona_sandboxes: dict = {}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def check(name: str, runloop: object, daytona: object, expected: object) -> None:
    report["cases"].append(
        {
            "name": name,
            "runloop": runloop,
            "daytona": daytona,
            "expected": expected,
            "pass": runloop == expected and daytona == expected,
        }
    )


def observe(name: str, actual: object, passed: bool) -> None:
    report["cases"].append({"name": name, "actual": actual, "pass": passed})


async def runloop_cmd(box, command: str) -> dict:
    result = await box.cmd.exec(command)
    return {
        "exit": result.exit_code,
        "stdout": await result.stdout(),
        "stderr": await result.stderr(),
    }


async def daytona_cmd(sandbox, command: str) -> dict:
    # Session commands keep stdout and stderr separate, unlike process.exec(). The
    # subshell stops a top-level `exit` from ending the session's shell.
    result = await sandbox.process.execute_session_command(
        SESSION,
        SessionExecuteRequest(command=f"({command})", run_async=False),
        timeout=120,
    )
    return {
        "exit": result.exit_code,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
    }


async def daytona_text(sandbox, path: str) -> str:
    return (await sandbox.fs.download_file(path) or b"").decode()


def alive(result: dict) -> bool:
    return result["exit"] == 0 and result["stdout"].rstrip("\n") == f"{WORKER} 600 "


def remove_daytona_sandbox(daytona, name: str):
    async def action() -> None:
        try:
            sandbox = daytona_sandboxes.get(name) or await daytona.get(name)
        except DaytonaNotFoundError:
            return  # create() failed before the sandbox existed.
        await sandbox.delete(wait=True, timeout=300)

    return action


def remove_daytona_snapshot(daytona, source, name: str):
    async def action() -> None:
        # Wait for any capture still running after create_snapshot() gave up, so the
        # delete applies to the finished snapshot.
        for _ in range(CAPTURE_TIMEOUT // 5):
            await source.refresh_data()
            if source.state != SandboxState.SNAPSHOTTING:
                break
            await asyncio.sleep(5)
        try:
            snapshot = await daytona.snapshot.get(name)
        except DaytonaNotFoundError:
            return  # The capture never created it.
        await daytona.snapshot.delete(snapshot)

    return action


async def create_daytona_sandbox(daytona, role: str, **params):
    key = "daytona" if role == "primary" else f"daytona{role.title()}"
    name = NAME if role == "primary" else f"{NAME}-{role}"
    # Register cleanup first: create() waits for startup and may raise after the
    # sandbox exists, so cleanup looks it up by name.
    cleanup.append((key, remove_daytona_sandbox(daytona, name)))
    sandbox = await daytona.create(
        CreateSandboxFromSnapshotParams(
            name=name,
            labels={**LABELS, "role": role},
            ttl_minutes=TTL_MINUTES,
            **params,
        ),
        timeout=600,
    )
    daytona_sandboxes[name] = sandbox
    await sandbox.process.create_session(SESSION)
    return sandbox


async def compare(extended: bool) -> None:
    stage = "setup"
    runloop = daytona = None
    try:
        if not (
            os.environ.get("RUNLOOP_API_KEY") and os.environ.get("DAYTONA_API_KEY")
        ):
            raise RuntimeError("credentials missing")
        for package in ("runloop-api-client", "daytona"):
            report["versions"][package] = version(package)
        report["startedAt"] = now()
        report["fixtureHash"] = sha256(CONTENT.encode())
        runloop = AsyncRunloopSDK()
        daytona = AsyncDaytona()

        stage = "Runloop creation"
        created = await runloop.api.devboxes.create(
            name=NAME, launch_parameters=KEEP_ALIVE
        )
        box = runloop.devbox.from_id(created.id)
        report["resources"]["runloop"] = box.id
        cleanup.append(("runloop", box.shutdown))
        await box.await_running()

        stage = "Daytona creation"
        sandbox = await create_daytona_sandbox(daytona, "primary")
        report["resources"]["daytona"] = sandbox.id
        report["daytonaSnapshot"] = sandbox.snapshot

        stage = "core comparisons"
        for code in (0, 7):
            command = (
                f"printf 'stdout-probe\\n'; printf 'stderr-probe\\n' >&2; exit {code}"
            )
            check(
                f"command exit {code}",
                await runloop_cmd(box, command),
                await daytona_cmd(sandbox, command),
                {"exit": code, "stdout": "stdout-probe\n", "stderr": "stderr-probe\n"},
            )
        command = "printf 'no-newline'; printf 'err' >&2"
        check(
            "output without trailing newline",
            await runloop_cmd(box, command),
            await daytona_cmd(sandbox, command),
            {"exit": 0, "stdout": "no-newline\n", "stderr": "err\n"},
        )
        await box.file.write(file_path=f"{PATH}.txt", contents=CONTENT)
        await sandbox.fs.upload_file(CONTENT.encode(), f"{PATH}.txt")
        check(
            "UTF-8 file round trip",
            await box.file.read(file_path=f"{PATH}.txt"),
            await daytona_text(sandbox, f"{PATH}.txt"),
            CONTENT,
        )

        stage = "Daytona command semantics"
        merged = await sandbox.process.exec("printf 'err' >&2; exit 7")
        observe(
            "Daytona process.exec returns stderr in result without a final newline",
            {"exit": merged.exit_code, "result": merged.result},
            merged.exit_code == 7 and merged.result == "err",
        )

        if extended:
            stage = "filesystem fixtures"
            check(
                "fixture creation",
                (await runloop_cmd(box, FIXTURE))["exit"],
                (await daytona_cmd(sandbox, FIXTURE))["exit"],
                0,
            )
            expected_manifest = {
                "exit": 0,
                "stdout": f"{sha256(BINARY)}  binary\n{sha256(LARGE)}  large\n750\nbinary\n",
                "stderr": "",
            }
            check(
                "binary, large file, mode and symlink manifest",
                await runloop_cmd(box, MANIFEST),
                await daytona_cmd(sandbox, MANIFEST),
                expected_manifest,
            )
            binary = await sandbox.fs.download_file(f"{PATH}/binary") or b""
            observe("Daytona binary SDK read", binary.hex(), binary == BINARY)

            stage = "network block-all"
            blocked_sandbox = await create_daytona_sandbox(
                daytona, "blocked", network_block_all=True
            )
            blocked = await daytona_cmd(blocked_sandbox, f"{CURL} https://example.com")
            observe(
                "Daytona block-all set at creation denies outbound",
                blocked,
                blocked["exit"] in DENIED,
            )

            stage = "network domain allow list"
            listed_sandbox = await create_daytona_sandbox(
                daytona, "allowlisted", domain_allow_list="example.com"
            )
            # Give the listed host up to three attempts while the new sandbox's allow
            # list warms up.
            for attempt in range(1, 4):
                allowed = await daytona_cmd(
                    listed_sandbox, f"{CURL} https://example.com"
                )
                if allowed["exit"] == 0:
                    break
                await asyncio.sleep(2)
            observe(
                "Daytona domain allow list permits listed host",
                {**allowed, "attempts": attempt},
                allowed["exit"] == 0,
            )
            unlisted = await daytona_cmd(
                listed_sandbox, f"{CURL} https://www.wikipedia.org"
            )
            observe(
                "Daytona domain allow list blocks unlisted host",
                unlisted,
                unlisted["exit"] in DENIED,
            )

            stage = "application file transfer"
            source = await box.file.read(file_path=f"{PATH}.txt")
            await sandbox.fs.upload_file(source.encode(), f"{PATH}/migrated.txt")
            migrated = await daytona_text(sandbox, f"{PATH}/migrated.txt")
            check("Runloop to Daytona file transfer", source, migrated, CONTENT)
            await box.file.write(file_path=f"{PATH}/returned.txt", contents=migrated)
            check(
                "Daytona to Runloop reverse transfer",
                await box.file.read(file_path=f"{PATH}/returned.txt"),
                migrated,
                CONTENT,
            )

            stage = "suspend/stop and resume/start"
            await runloop_cmd(box, START_WORKER)
            await daytona_cmd(sandbox, START_WORKER)
            check(
                "background process running before suspend/stop",
                alive(await runloop_cmd(box, WORKER_IDENTITY)),
                alive(await daytona_cmd(sandbox, WORKER_IDENTITY)),
                True,
            )
            await box.suspend()
            await box.await_suspended()
            await sandbox.stop(timeout=300)
            await box.resume()
            await sandbox.start(timeout=300)
            await sandbox.process.create_session(SESSION)
            check(
                "disk after resume/start",
                await box.file.read(file_path=f"{PATH}.txt"),
                await daytona_text(sandbox, f"{PATH}.txt"),
                CONTENT,
            )
            check(
                "background process alive after resume/start",
                alive(await runloop_cmd(box, WORKER_IDENTITY)),
                alive(await daytona_cmd(sandbox, WORKER_IDENTITY)),
                False,
            )

            stage = "snapshots"
            runloop_snapshot = await box.snapshot_disk_async(name=NAME)
            report["resources"]["runloopSnapshot"] = runloop_snapshot.id
            cleanup.append(("runloopSnapshot", runloop_snapshot.delete))
            await runloop_snapshot.await_completed()
            cleanup.append(
                ("daytonaSnapshot", remove_daytona_snapshot(daytona, sandbox, NAME))
            )
            await sandbox.create_snapshot(NAME, timeout=CAPTURE_TIMEOUT)

            stage = "snapshot restore"
            restored_data = await runloop.api.devboxes.create(
                snapshot_id=runloop_snapshot.id, launch_parameters=KEEP_ALIVE
            )
            restored_box = runloop.devbox.from_id(restored_data.id)
            report["resources"]["runloopRestored"] = restored_box.id
            cleanup.append(("runloopRestored", restored_box.shutdown))
            await restored_box.await_running()
            restored = await create_daytona_sandbox(daytona, "restored", snapshot=NAME)
            report["resources"]["daytonaRestored"] = restored.id
            check(
                "restored filesystem manifest",
                await runloop_cmd(restored_box, MANIFEST),
                await daytona_cmd(restored, MANIFEST),
                expected_manifest,
            )
    except Exception as error:
        report["errors"].append({"stage": stage, "type": type(error).__name__})
    finally:
        for name, action in reversed(cleanup):
            try:
                await action()
                report["cleanup"][name] = "ok"
            except Exception as error:
                report["cleanup"][name] = f"failed: {type(error).__name__}"
        if daytona is not None:
            await daytona.close()
        if runloop is not None:
            await runloop.aclose()


def write_report(path: Path) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(report, indent=2, ensure_ascii=False) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output", type=Path, default=Path("transition-results_private.json")
    )
    parser.add_argument("--extended", action="store_true")
    args = parser.parse_args()
    asyncio.run(compare(args.extended))
    report["finishedAt"] = now()
    write_report(args.output)
    passed = sum(case["pass"] for case in report["cases"])
    print(f"Wrote {args.output}; {passed}/{len(report['cases'])} checks passed")
    if report["errors"]:
        return 2
    return (
        0
        if all(case["pass"] for case in report["cases"])
        and all(status == "ok" for status in report["cleanup"].values())
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
