# Runloop to Daytona transition probe

This starter uses the Runloop and Daytona Python SDKs to compare isolated devboxes and sandboxes. It records JSON evidence and attempts cleanup even after errors. It does not migrate an existing customer environment or establish complete production equivalence.

## Run

Use Python 3.10 or later and isolated projects. Set `RUNLOOP_API_KEY` and `DAYTONA_API_KEY` in your shell. The script creates billable resources; never commit credentials or raw results.

```sh
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python compare_core_public.py --output transition-results_private.json
python compare_core_public.py --extended --output transition-results_private.json
```

The default probe checks successful and nonzero command exits, stdout/stderr, output without a trailing newline, and a UTF-8 file round trip. It also records that Daytona's `process.exec()` returns stderr inside its single result string. Extended mode adds binary and 1 MiB fixtures, modes and symlinks, sandboxes created with outbound block-all and a domain allow list, application-file transfer in both directions, Runloop suspend/resume against Daytona stop/start, and native snapshots and restoration.

Daytona commands run as session commands wrapped in a subshell. Like Runloop's `cmd.exec()`, session commands return stdout and stderr separately and end unterminated output with a newline; the subshell stops a command's `exit` from ending the session's shell.

Daytona sandboxes start from the organization's default snapshot with a 60-minute wall-clock TTL (`ttl_minutes`), so Daytona destroys them even if cleanup fails. Runloop devboxes have an explicit one-hour keep-alive fallback. Every Daytona sandbox gets a unique name, and cleanup is registered before each create call; if `create()` raises before returning a sandbox, cleanup looks the sandbox up by that name. The script attempts explicit cleanup of every resource it created, including both snapshots, and never deletes pre-existing resources. Snapshot cleanup first waits for any Daytona capture still in progress.

The network checks need a Tier 3 or Tier 4 Daytona organization; on Tier 1 and 2, organization network restrictions take precedence over sandbox settings.

Exit `0` means all assertions and cleanup passed, `1` means a mismatch or cleanup failure, and `2` means an API/setup error. Inspect `errors`, `resources`, and `cleanup` even after a failed run. The report preserves stage and error type without printing SDK exception bodies, which can contain request credentials. Failed cleanup requires manual removal using the recorded IDs. The output file is created with mode `0600`; do not reuse an existing world-readable output file.

## Scope and next steps

The [Daytona transition guide draft](docs/runloop-to-daytona.mdx) maps devboxes, Axons, blueprints, snapshots, and evaluations. The [validation plan draft](docs/runloop-to-daytona-validation.mdx) covers workload-specific session behavior, inbound authentication, idle timeouts, environment rebuilding, external databases, event recovery, and evaluation scoring. Those checks need the customer's own application and acceptance criteria.

The probe covers Daytona's default container sandboxes. Linux VM sandboxes, which add pause/resume with memory and forking, need a VM snapshot available to your organization and region; extend the probe before relying on them.

Native snapshot comparisons do not demonstrate cross-provider snapshot import. The file-transfer probe copies one UTF-8 application file through authenticated SDK calls; larger datasets, modes, ownership, and database consistency need a migration tool designed for that workload.

## Recorded live evidence

On October 18, 2026 (PT), the extended probe passed **17/17 assertions** with `runloop-api-client` 2.0.0 and `daytona` 0.225.0. All eight devbox, sandbox, and snapshot cleanup actions succeeded. [validation-summary.json](validation-summary.json) contains the sanitized case results and timestamps; raw resource IDs and fixture output remain private.

A live fault-injection run made Daytona's `create_snapshot()` time out while the capture was still running: exit 2 was returned, cleanup waited for the capture to finish before deleting the snapshot, all six registered resources were cleaned up, and no Daytona sandbox or snapshot remained afterwards. Cleanup for the Daytona snapshot is registered by name before capture, so it remains available when the capture call raises.

A creation-failure injection raised after Daytona created the sandbox but before `create()` returned. Exit 2 was returned, and cleanup found the sandbox by name and removed it.
