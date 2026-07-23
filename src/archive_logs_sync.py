"""archive_logs_sync.py - NEW CODE (not part of the sfmc-api package).

sfmc-api's `pull_new_downloads` module is scoped specifically to the
`from-glider` folder (see its own docstring / docs/pull_new_downloads.md).
It does not cover `archive` or `logs`. This module fills that one gap,
reusing sfmc-api's SFMCClient for everything it can do correctly
(auth, the STOMP connection-event stream, the download-glider-files
call) rather than re-implementing any of that.

Design mirrors the JS pipeline's downloadGliderFiles.js:
  - Try an incremental download first (last_modified_after = last
    success time minus a safety margin, in the confirmed-correct
    "yyyyMMddHHmm" format).
  - Fall back to a full download if there's no prior cursor yet, or
    if the incremental call fails for any reason.
  - Extract with stdlib zipfile, diff against a manifest keyed by
    filename -> size, copy only new/changed files to the real
    destination.
  - Skip DOS 8.3 partial-transfer artifacts (e.g. *.scd/*.tcd/*.mcd)
    before they're ever written or added to the manifest.

State/manifest are plain JSON files, same shape as the JS version, so
this is inspectable the same way: state/download_state.json and
state/download_manifest.json.
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sfmc_api import SFMCClient
from sfmc_api.exceptions import SFMCError

logger = logging.getLogger("archive_logs_sync")

# Same default as the JS version - matches DOS 8.3 partial-transfer names
# like "01800013.scd" / ".tcd" / ".mcd". Basename-only match.
DEFAULT_EXCLUDE_FILE_PATTERN = r"^[^/\\]{1,8}\.[A-Za-z]cd$"

# Confirmed-correct format for lastModifiedAfter: plain yyyyMMddHHmm,
# no punctuation. See docs/pull_new_downloads.md and client.py's own
# docstring for download_glider_files - both agree on this format.
_CUTOFF_FMT = "%Y%m%d%H%M"


@dataclass
class SyncConfig:
    local_base_path: Path
    filter: str = "*"
    margin_minutes: int = 2880  # 48h, matches sfmc-api's own proven default
    exclude_file_pattern: str = DEFAULT_EXCLUDE_FILE_PATTERN


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    content = path.read_text(encoding="utf-8").strip()
    return json.loads(content) if content else {}


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _to_cutoff(dt: datetime) -> str:
    return dt.strftime(_CUTOFF_FMT)


def _is_valid_zip(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0 and zipfile.is_zipfile(path)


def sync_folder(
    client: SFMCClient,
    glider_name: str,
    folder: str,
    sync_config: SyncConfig,
    state_path: Path,
    manifest_path: Path,
    trigger: str = "disconnect",
) -> dict:
    """Sync one glider/folder. Returns a summary dict, same shape as the JS version."""
    state = _load_json(state_path)
    manifest = _load_json(manifest_path)

    glider_state = state.setdefault(glider_name, {})
    folder_state = glider_state.setdefault(folder, {})
    glider_manifest = manifest.setdefault(glider_name, {})
    folder_manifest = glider_manifest.setdefault(folder, {})

    destination_dir = sync_config.local_base_path / glider_name / folder
    destination_dir.mkdir(parents=True, exist_ok=True)

    prior_success_raw = folder_state.get("lastSuccessfulDownloadAt")
    prior_success = datetime.fromisoformat(prior_success_raw) if prior_success_raw else None

    used_incremental = False
    archive_size = 0

    with tempfile.TemporaryDirectory(prefix=f"sfmc_{glider_name}_{folder}_") as tmp_dir_str:
        tmp_dir = Path(tmp_dir_str)
        zip_path = tmp_dir / "download.zip"

        if prior_success is not None:
            cutoff_dt = prior_success - timedelta(minutes=sync_config.margin_minutes)
            cutoff = _to_cutoff(cutoff_dt)
            logger.info(
                "Downloading %s/%s incrementally since %s (margin %dm)",
                glider_name, folder, cutoff, sync_config.margin_minutes,
            )
            try:
                client.download_glider_files(
                    glider_name, folder, zip_path,
                    filter=sync_config.filter, last_modified_after=cutoff,
                )
                used_incremental = _is_valid_zip(zip_path)
                if not used_incremental:
                    logger.warning(
                        "%s/%s: incremental download returned an invalid response, "
                        "falling back to full download.", glider_name, folder,
                    )
            except SFMCError as error:
                logger.warning(
                    "%s/%s: incremental download failed (%s), falling back to full download.",
                    glider_name, folder, error,
                )

        if not used_incremental:
            logger.info(
                "Downloading full %s/%s (%s)",
                glider_name, folder,
                "fallback" if prior_success is not None else "no prior cursor yet",
            )
            try:
                client.download_glider_files(
                    glider_name, folder, zip_path, filter=sync_config.filter,
                )
            except SFMCError as error:
                folder_state["lastAttemptAt"] = datetime.now(UTC).isoformat()
                folder_state["lastAttemptStatus"] = "failed"
                folder_state["lastError"] = str(error)
                _save_json(state_path, state)
                logger.error("%s/%s: download failed: %s", glider_name, folder, error)
                return {"folder": folder, "status": "failed", "error": str(error)}

        if not _is_valid_zip(zip_path):
            folder_state["lastAttemptAt"] = datetime.now(UTC).isoformat()
            folder_state["lastAttemptStatus"] = "failed"
            folder_state["lastError"] = "Response was not a valid zip archive."
            _save_json(state_path, state)
            logger.error("%s/%s: response was not a valid zip archive.", glider_name, folder)
            return {"folder": folder, "status": "failed", "error": "invalid zip"}

        extract_dir = tmp_dir / "extracted"
        extract_dir.mkdir()
        archive_size = zip_path.stat().st_size
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extract_dir)

        exclude_regex = re.compile(sync_config.exclude_file_pattern, re.IGNORECASE)

        new_count = updated_count = unchanged_count = skipped_count = 0
        skipped_examples: list[str] = []

        for extracted_file in sorted(extract_dir.rglob("*")):
            if not extracted_file.is_file():
                continue
            rel_path = str(extracted_file.relative_to(extract_dir))

            if exclude_regex.match(extracted_file.name):
                skipped_count += 1
                if len(skipped_examples) < 5:
                    skipped_examples.append(rel_path)
                continue

            size = extracted_file.stat().st_size
            existing = folder_manifest.get(rel_path)
            is_new = existing is None
            is_changed = existing is not None and existing.get("size") != size

            if not is_new and not is_changed:
                unchanged_count += 1
                continue

            dest_path = destination_dir / rel_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(extracted_file, dest_path)

            folder_manifest[rel_path] = {
                "size": size,
                "syncedAt": datetime.now(UTC).isoformat(),
            }
            if is_new:
                new_count += 1
            else:
                updated_count += 1

    now_iso = datetime.now(UTC).isoformat()
    folder_state["lastSuccessfulDownloadAt"] = now_iso
    folder_state["lastAttemptAt"] = now_iso
    folder_state["lastAttemptStatus"] = "downloaded"
    folder_state["lastTrigger"] = trigger
    folder_state["lastDownloadMode"] = "incremental" if used_incremental else "full"
    folder_state["lastDownloadSizeBytes"] = archive_size
    folder_state["lastNewFiles"] = new_count
    folder_state["lastUpdatedFiles"] = updated_count
    folder_state["lastUnchangedFiles"] = unchanged_count
    folder_state["lastSkippedFiles"] = skipped_count
    folder_state.pop("lastError", None)

    _save_json(state_path, state)
    _save_json(manifest_path, manifest)

    logger.info(
        "%s/%s: synced (%s). %d new, %d updated, %d unchanged, %d skipped (partial-transfer).",
        glider_name, folder, "incremental" if used_incremental else "full",
        new_count, updated_count, unchanged_count, skipped_count,
    )
    if skipped_examples:
        logger.info("  e.g. skipped: %s", ", ".join(skipped_examples))

    return {
        "folder": folder,
        "status": "downloaded" if (new_count + updated_count) > 0 else "no-new-files",
        "downloadMode": "incremental" if used_incremental else "full",
        "newCount": new_count,
        "updatedCount": updated_count,
        "unchangedCount": unchanged_count,
        "skippedCount": skipped_count,
    }


def sync_all_folders(
    client: SFMCClient,
    glider_name: str,
    folders: list[str],
    sync_config: SyncConfig,
    state_path: Path,
    manifest_path: Path,
    trigger: str = "disconnect",
) -> list[dict]:
    return [
        sync_folder(client, glider_name, folder, sync_config, state_path, manifest_path, trigger)
        for folder in folders
    ]
