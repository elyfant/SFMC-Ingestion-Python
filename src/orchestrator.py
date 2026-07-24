"""orchestrator.py - the main entrypoint.

Ties together two things for each configured glider:

1. `from-glider`: delegated entirely to sfmc-api's own
   `sfmc-pull-new-downloads` CLI, run as a subprocess. That tool
   already solves this folder's real complexity correctly (Dinkum
   renames, partial-transfer deferral, settle windows) - there is no
   reason to reimplement it, so we don't.

2. `archive` / `logs`: NOT covered by sfmc-api. Handled by our own
   archive_logs_sync.py, run in a background thread per glider, using
   SFMCClient directly for auth + the connection-event stream + the
   download call - all real sfmc-api calls, just our own trigger
   logic wrapped around them (mirrors the JS index.js state machine:
   sync once at startup, then again on every disconnect event).

CAVEAT: the STOMP-streaming half of this (SFMCClient.open_stream /
subscribe_connection_events) could not be executed in the sandbox
this was written in - no network access there to install sfmc-api's
own dependencies (httpx, websockets). The archive_logs_sync module
itself (the download/extract/manifest-diff logic) WAS tested against
fixture zips with a fake client. Smoke-test this file for real before
trusting it unattended - see the smoke-test note at the bottom.
"""

from __future__ import annotations

import argparse
import json
import logging
import logging.handlers
import signal
import subprocess
import threading
from datetime import UTC, datetime
from pathlib import Path

from sfmc_api import SFMCClient
from sfmc_api.exceptions import SFMCError

from archive_logs_sync import SyncConfig, sync_all_folders
from trigger_processing import watch_and_trigger

logger = logging.getLogger("orchestrator")

_shutdown = threading.Event()


def _load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _state_paths(base_dir: Path) -> tuple[Path, Path]:
    return base_dir / "state" / "download_state.json", base_dir / "state" / "download_manifest.json"


def run_pull_new_downloads(glider_name: str, host: str, output_dir: Path, extra_args: list[str], log_dir: Path) -> None:
    """Run sfmc-api's own from-glider tool as a subprocess, restarting it if it
    ever exits unexpectedly (network blip, SFMC restart, etc). This is a real,
    separate process - not our code - so a crash in it can't take down the
    archive/logs thread for the same glider.

    Its own stdout/stderr is captured to log_dir/pull-new-downloads-<glider>.log
    (appended, not overwritten) rather than inherited from the parent process -
    it doesn't go through Python's logging module, so it needs its own file or
    it only exists in whatever terminal launched the orchestrator.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"pull-new-downloads-{glider_name}.log"
    cmd = [
        "sfmc-pull-new-downloads",  # installed console script (pyproject.toml entry point)
        "--host", host,
        *extra_args,
        glider_name, str(output_dir),
    ]

    backoff_seconds = 5
    while not _shutdown.is_set():
        logger.info(
            "[%s] starting pull-new-downloads: %s (output -> %s)",
            glider_name, " ".join(cmd), log_path,
        )
        with open(log_path, "a") as log_file:
            log_file.write(f"\n=== started {datetime.now(UTC).isoformat()} ===\n")
            log_file.flush()
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            proc.wait()
        if _shutdown.is_set():
            break
        logger.warning(
            "[%s] pull-new-downloads exited (code %s), restarting in %ds",
            glider_name, proc.returncode, backoff_seconds,
        )
        _shutdown.wait(backoff_seconds)
        backoff_seconds = min(backoff_seconds * 2, 300)


def run_archive_logs_sync(
    glider_name: str,
    client: SFMCClient,
    folders: list[str],
    sync_config: SyncConfig,
    state_path: Path,
    manifest_path: Path,
) -> None:
    """Startup sync, then sync again on every disconnect event, forever.
    Mirrors the JS index.js state machine for the folders sfmc-api doesn't cover.
    """
    logger.info("[%s] running startup sync for %s", glider_name, folders)
    results = sync_all_folders(client, glider_name, folders, sync_config, state_path, manifest_path, trigger="startup")
    logger.info("[%s] startup sync done: %s", glider_name, results)

    backoff_seconds = 15
    while not _shutdown.is_set():
        try:
            # open_stream() only signs in if NO token is cached yet - it never
            # checks whether an already-cached one has expired (see
            # SFMCClient._ensure_auth). Left alone, a stream that dies after
            # the cached token expires (e.g. overnight) retries forever with
            # the same dead token and 401s permanently. Forcing a fresh
            # authenticate() here before every attempt makes each retry
            # self-healing regardless of *why* the previous connection died.
            client.authenticate()
            with client.open_stream() as stomp:
                sub = client.subscribe_connection_events(glider_name, stomp)
                backoff_seconds = 15  # reset after a successful (re)connect
                logger.info("[%s] listening for connection events (archive/logs)", glider_name)
                for events in sub:
                    if _shutdown.is_set():
                        break
                    for event in events:
                        if event.get("active") is False:
                            connection_id = event.get("id")
                            logger.info(
                                "[%s] disconnected (connection %s) - syncing %s",
                                glider_name, connection_id, folders,
                            )
                            results = sync_all_folders(
                                client, glider_name, folders, sync_config,
                                state_path, manifest_path, trigger="disconnect",
                            )
                            logger.info("[%s] disconnect sync done: %s", glider_name, results)
        except SFMCError as error:
            logger.warning(
                "[%s] connection-event stream failed (%s), reconnecting in %ds",
                glider_name, error, backoff_seconds,
            )
            _shutdown.wait(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 300)


def main() -> None:
    parser = argparse.ArgumentParser(description="HYDRA RV glider ingestion orchestrator")
    parser.add_argument("--config", type=Path, default=Path("config/app.json"))
    parser.add_argument(
        "--credentials", type=Path, default=None,
        help="Override the credentialsPath set in config/app.json (usually not needed).",
    )
    args = parser.parse_args()

    log_dir = args.config.parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.handlers.RotatingFileHandler(
                log_dir / "orchestrator.log", maxBytes=10_000_000, backupCount=5,
            ),
        ],
    )

    config = _load_config(args.config)
    base_dir = args.config.parent.parent
    state_path, manifest_path = _state_paths(base_dir)
    local_base_path = Path(config["localBasePath"])

    sync_config = SyncConfig(
        local_base_path=local_base_path,
        filter=config.get("filter", "*"),
        margin_minutes=config.get("marginMinutes", 2880),
        exclude_file_pattern=config.get("excludeFilePattern", SyncConfig.__dataclass_fields__["exclude_file_pattern"].default),
    )

    credentials_path = Path(config.get("credentialsPath", "credentials.json"))
    if not credentials_path.is_absolute():
        credentials_path = (args.config.parent / credentials_path).resolve()
    if not credentials_path.exists():
        example_path = credentials_path.with_name(credentials_path.stem + ".example.json")
        raise SystemExit(
            f"Credentials file not found: {credentials_path}\n"
            f"Copy {example_path.name} to {credentials_path.name} in the same "
            f"folder and fill in your real clientId/secret from "
            f"https://{config['host']}/sfmc/api-access-pages/api-access"
        )

    inkfish_root_raw = config.get("inkfishRoot")
    if inkfish_root_raw:
        inkfish_root = Path(inkfish_root_raw)
        if not inkfish_root.is_absolute():
            inkfish_root = (args.config.parent / inkfish_root).resolve()
        if not inkfish_root.exists():
            logger.warning(
                "inkfishRoot is set to %s but that path doesn't exist - "
                "Inkfish processing trigger will be disabled.", inkfish_root,
            )
            inkfish_root = None
    else:
        inkfish_root = None
        logger.info("inkfishRoot not set in config - Inkfish processing trigger disabled.")

    companion_folders = list(config["companionFolders"])
    if "from-glider" in companion_folders:
        logger.warning(
            "'from-glider' is in companionFolders but is already handled by "
            "pull-new-downloads - removing it from the archive/logs sync to "
            "avoid two processes independently downloading into the same folder."
        )
        companion_folders = [f for f in companion_folders if f != "from-glider"]

    def handle_shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping...")
        _shutdown.set()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    threads: list[threading.Thread] = []
    extra_args = config.get("pullNewDownloadsExtraArgs", [])
    # config's credentialsPath is the default for both the subprocess and our
    # in-process client; an explicit --credentials flag can still override it.
    extra_args = [*extra_args, "--credentials", str(args.credentials or credentials_path)]

    for glider_name in config["gliders"]:
        # One SFMCClient per glider, pointed at the same consolidated
        # credentials file as the pull-new-downloads subprocess below.
        client = SFMCClient(config_path=credentials_path, host=config["host"])

        from_glider_dir = local_base_path / glider_name / "from-glider"
        t1 = threading.Thread(
            target=run_pull_new_downloads,
            args=(glider_name, config["host"], from_glider_dir, extra_args, log_dir),
            daemon=True,
            name=f"pull-new-downloads-{glider_name}",
        )
        t2 = threading.Thread(
            target=run_archive_logs_sync,
            args=(glider_name, client, companion_folders, sync_config, state_path, manifest_path),
            daemon=True,
            name=f"archive-logs-sync-{glider_name}",
        )
        threads.extend([t1, t2])
        t1.start()
        t2.start()

        if inkfish_root is not None:
            t3 = threading.Thread(
                target=watch_and_trigger,
                kwargs=dict(
                    glider_name=glider_name,
                    from_glider_dir=from_glider_dir,
                    inkfish_root=inkfish_root,
                    run_gliders_script=config.get("runGlidersScript", "run_gliders.py"),
                    poll_seconds=config.get("triggerPollSeconds", 60),
                    python_executable=config.get("pythonExecutable", "python3"),
                    shutdown_event=_shutdown,
                ),
                daemon=True,
                name=f"trigger-processing-{glider_name}",
            )
            threads.append(t3)
            t3.start()

    logger.info("Orchestrator running. Ctrl-C to stop.")
    while not _shutdown.is_set():
        _shutdown.wait(1)

    logger.info("Waiting for threads to stop...")
    for t in threads:
        t.join(timeout=10)


if __name__ == "__main__":
    main()

