"""trigger_processing.py - NEW CODE, not part of sfmc-api.

Watches a glider's from-glider directory and triggers Inkfish's
run_gliders.py whenever new files show up.

Why polling instead of hooking pull_new_downloads directly: that tool
downloads asynchronously on its own schedule (settle windows after a
surfacing, plus a 15-min idle reconcile) - there's no single moment we
can hook into that reliably means "no more files are coming for a
while". Polling the directory itself sidesteps needing to know that.

This is deliberately decoupled from archive_logs_sync.py's disconnect-
driven sync - a separate concern watching a separate directory.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import time
from pathlib import Path

logger = logging.getLogger("trigger_processing")


def _dir_signature(folder: Path) -> tuple[int, int]:
    """(file count, total bytes). Cheap local stat, no SFMC calls - safe to
    poll frequently. Precision doesn't matter much here: Inkfish's own
    pipeline is fingerprint-based and skips unchanged segments, so an
    occasional unnecessary trigger just costs a fast no-op run, not
    reprocessing."""
    if not folder.exists():
        return (0, 0)
    files = [f for f in folder.iterdir() if f.is_file()]
    return (len(files), sum(f.stat().st_size for f in files))


def watch_and_trigger(
    glider_name: str,
    from_glider_dir: Path,
    inkfish_root: Path,
    run_gliders_script: str = "run_gliders.py",
    poll_seconds: int = 60,
    python_executable: str = "python3",
    shutdown_event: threading.Event | None = None,
) -> None:
    """Runs forever (until shutdown_event is set). Call in its own thread,
    one per glider."""
    run_lock = threading.Lock()

    def run_processing() -> None:
        try:
            logger.info("[%s] triggering %s", glider_name, run_gliders_script)
            result = subprocess.run(
                [python_executable, run_gliders_script, glider_name],
                cwd=inkfish_root,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.error(
                    "[%s] %s exited %d\nstdout:\n%s\nstderr:\n%s",
                    glider_name, run_gliders_script, result.returncode,
                    result.stdout[-2000:], result.stderr[-2000:],
                )
            else:
                logger.info("[%s] %s completed OK", glider_name, run_gliders_script)
        except Exception:
            logger.exception("[%s] failed to run %s", glider_name, run_gliders_script)
        finally:
            run_lock.release()

    def try_trigger() -> bool:
        """Attempt to start a run. Returns True if one was actually started
        (so the caller can advance its 'last seen' signature), False if a
        run is already in progress (so the caller should leave its
        signature stale and retry next poll - otherwise a change that
        arrives mid-run could be silently forgotten once that run finishes)."""
        if not run_lock.acquire(blocking=False):
            logger.info(
                "[%s] run_gliders.py already in progress, will retry next poll.",
                glider_name,
            )
            return False
        threading.Thread(target=run_processing, daemon=True).start()
        return True

    logger.info(
        "[%s] watching %s every %ds, will run: %s %s %s (cwd=%s)",
        glider_name, from_glider_dir, poll_seconds,
        python_executable, run_gliders_script, glider_name, inkfish_root,
    )

    event = shutdown_event or threading.Event()

    # Trigger once at startup too - otherwise data that arrived before this
    # watcher started (e.g. the very first run of a cruise) never gets an
    # initial processing pass; it would just sit there until the next change.
    last_signature = _dir_signature(from_glider_dir)
    logger.info("[%s] startup trigger (%d files, %d bytes)", glider_name, *last_signature)
    try_trigger()

    while not event.is_set():
        event.wait(poll_seconds)
        signature = _dir_signature(from_glider_dir)
        if signature != last_signature:
            logger.info(
                "[%s] from-glider changed (%d files, %d bytes) -> triggering",
                glider_name, *signature,
            )
            if try_trigger():
                last_signature = signature
            # else: leave last_signature as-is so this change is retried next poll
