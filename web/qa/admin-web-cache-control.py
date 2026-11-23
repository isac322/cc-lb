#!/usr/bin/env python3
"""Measure and control Linux filesystem page cache for isolated QA files.

The Main QA harness creates every fixture, ownership marker, and manifest and
owns database process lifecycle. This tool never creates, deletes, discovers,
or modifies database contents. It only inspects, sequentially reads, fsyncs,
and advises Linux about explicitly named regular files in one owned volume.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import hmac
import json
import math
import os
import posixpath
import re
import stat
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence


OWNER_MARKER = ".admin-web-cache-control-owner.json"
OWNER_SCHEMA = "cc-lb-admin-web-cache-control-root/v1"
MANIFEST_SCHEMA = "cc-lb-admin-web-cache-control-manifest/v1"
REPORT_SCHEMA = "cc-lb-admin-web-cache-control-report/v1"
MAX_CONTROL_FILE_BYTES = 1_048_576
MAX_TARGET_FILES = 4_096
DEFAULT_READ_CHUNK_BYTES = 1_048_576
MAX_READ_CHUNK_BYTES = 16_777_216
MINCORE_WINDOW_BYTES = 268_435_456
POSTGRES_RELATION = re.compile(
    r"^base/[1-9][0-9]*/[1-9][0-9]*(?:_(?:fsm|vm|init))?(?:\.[0-9]+)?$"
)


class CacheControlError(RuntimeError):
    pass


@dataclass
class OwnedRoot:
    path: Path
    descriptor: int
    device: int
    owner_uid: int


@dataclass
class Target:
    relative_path: str
    descriptor: int
    initial_stat: os.stat_result


class LinuxPageCache:
    PROT_NONE = 0
    MAP_SHARED = 1
    POSIX_FADV_DONTNEED = 4

    def __init__(self) -> None:
        if sys.platform != "linux":
            raise CacheControlError("this page-cache tool requires Linux")
        self.page_size = os.sysconf("SC_PAGE_SIZE")
        if not isinstance(self.page_size, int) or self.page_size <= 0:
            raise CacheControlError("cannot determine the Linux page size")
        self.libc = ctypes.CDLL(None, use_errno=True)
        try:
            self.libc.mmap.argtypes = (
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_longlong,
            )
            self.libc.mmap.restype = ctypes.c_void_p
            self.libc.mincore.argtypes = (
                ctypes.c_void_p,
                ctypes.c_size_t,
                ctypes.POINTER(ctypes.c_ubyte),
            )
            self.libc.mincore.restype = ctypes.c_int
            self.libc.munmap.argtypes = (ctypes.c_void_p, ctypes.c_size_t)
            self.libc.munmap.restype = ctypes.c_int
            self.libc.posix_fadvise.argtypes = (
                ctypes.c_int,
                ctypes.c_longlong,
                ctypes.c_longlong,
                ctypes.c_int,
            )
            self.libc.posix_fadvise.restype = ctypes.c_int
        except AttributeError as error:
            raise CacheControlError(
                "Linux libc does not expose mmap, mincore, munmap, and posix_fadvise"
            ) from error
        self.map_failed = ctypes.c_void_p(-1).value

    def resident_pages(self, descriptor: int, size: int) -> int:
        if size < 0:
            raise CacheControlError("file size cannot be negative")
        if size == 0:
            return 0
        resident = 0
        offset = 0
        while offset < size:
            length = min(MINCORE_WINDOW_BYTES, size - offset)
            pages = math.ceil(length / self.page_size)
            vector = (ctypes.c_ubyte * pages)()
            ctypes.set_errno(0)
            address = self.libc.mmap(
                None,
                length,
                self.PROT_NONE,
                self.MAP_SHARED,
                descriptor,
                offset,
            )
            if address == self.map_failed:
                code = ctypes.get_errno() or errno.EIO
                raise OSError(code, os.strerror(code))
            try:
                ctypes.set_errno(0)
                if self.libc.mincore(address, length, vector) != 0:
                    code = ctypes.get_errno() or errno.EIO
                    raise OSError(code, os.strerror(code))
                resident += sum(1 for value in vector if value & 1)
            finally:
                ctypes.set_errno(0)
                if self.libc.munmap(address, length) != 0:
                    code = ctypes.get_errno() or errno.EIO
                    raise OSError(code, os.strerror(code))
            offset += length
        return resident

    def evict(self, descriptor: int) -> int:
        return self.libc.posix_fadvise(
            descriptor,
            0,
            0,
            self.POSIX_FADV_DONTNEED,
        )


def required_text(raw: str | None, label: str) -> str:
    if raw is None or not raw.strip():
        raise CacheControlError(f"{label} must not be empty")
    if "\x00" in raw:
        raise CacheControlError(f"{label} must not contain a null byte")
    return raw


def normalize_relative_path(raw: str | None, label: str) -> str:
    value = required_text(raw, label)
    if value.startswith("/"):
        raise CacheControlError(f"{label} must be relative to --root")
    normalized = posixpath.normpath(value)
    if normalized != value:
        raise CacheControlError(f"{label} must already be normalized: {value!r}")
    parts = value.split("/")
    if value == "." or any(part in {"", ".", ".."} for part in parts):
        raise CacheControlError(f"{label} must name a file below --root")
    return value


def open_root(raw: str | None) -> OwnedRoot:
    value = required_text(raw, "--root")
    absolute = os.path.normpath(os.path.abspath(os.path.expanduser(value)))
    if absolute == os.path.sep:
        raise CacheControlError("--root must not be the filesystem root")
    components = [part for part in absolute.split(os.path.sep) if part]
    descriptor = os.open(
        os.path.sep,
        os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC,
    )
    try:
        for component in components:
            next_descriptor = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = next_descriptor
        root_stat = os.fstat(descriptor)
        effective_uid = os.geteuid()
        if root_stat.st_uid != effective_uid:
            raise CacheControlError(
                f"--root owner uid {root_stat.st_uid} does not match effective uid {effective_uid}"
            )
        return OwnedRoot(
            path=Path(absolute),
            descriptor=descriptor,
            device=root_stat.st_dev,
            owner_uid=root_stat.st_uid,
        )
    except Exception:
        os.close(descriptor)
        raise


def open_beneath(
    root: OwnedRoot,
    relative_path: str,
    final_flags: int,
) -> int:
    parts = relative_path.split("/")
    directory = os.dup(root.descriptor)
    try:
        for component in parts[:-1]:
            next_directory = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=directory,
            )
            directory_stat = os.fstat(next_directory)
            if directory_stat.st_dev != root.device:
                os.close(next_directory)
                raise CacheControlError(
                    f"path crosses out of the owned volume: {relative_path}"
                )
            os.close(directory)
            directory = next_directory
        descriptor = os.open(
            parts[-1],
            final_flags | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=directory,
        )
        file_stat = os.fstat(descriptor)
        if file_stat.st_dev != root.device:
            os.close(descriptor)
            raise CacheControlError(
                f"path crosses out of the owned volume: {relative_path}"
            )
        return descriptor
    finally:
        os.close(directory)


def read_control_json(
    root: OwnedRoot,
    relative_path: str,
    label: str,
) -> tuple[dict[str, Any], os.stat_result]:
    descriptor = open_beneath(root, relative_path, os.O_RDONLY)
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise CacheControlError(f"{label} must be a regular file")
        if file_stat.st_uid != root.owner_uid:
            raise CacheControlError(f"{label} must be owned by the effective uid")
        if file_stat.st_nlink != 1:
            raise CacheControlError(f"{label} must not have hard links")
        if file_stat.st_size > MAX_CONTROL_FILE_BYTES:
            raise CacheControlError(f"{label} exceeds {MAX_CONTROL_FILE_BYTES} bytes")
        chunks: list[bytes] = []
        remaining = MAX_CONTROL_FILE_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(65_536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > MAX_CONTROL_FILE_BYTES:
            raise CacheControlError(f"{label} exceeds {MAX_CONTROL_FILE_BYTES} bytes")
        final_stat = os.fstat(descriptor)
        if stable_identity(final_stat) != stable_identity(file_stat):
            raise CacheControlError(f"{label} changed while it was read")
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise CacheControlError(f"{label} is not valid UTF-8 JSON") from error
        if not isinstance(value, dict):
            raise CacheControlError(f"{label} must contain a JSON object")
        return value, file_stat
    finally:
        os.close(descriptor)


def verify_owner_marker(root: OwnedRoot, raw_nonce: str | None) -> None:
    nonce = required_text(raw_nonce, "--nonce")
    if nonce != nonce.strip() or not 16 <= len(nonce) <= 256:
        raise CacheControlError("--nonce must contain 16 to 256 non-whitespace characters")
    marker, marker_stat = read_control_json(root, OWNER_MARKER, "ownership marker")
    if marker_stat.st_mode & 0o022:
        raise CacheControlError("ownership marker must not be group/world writable")
    expected_fields = {"schema", "nonce", "owner_uid"}
    if set(marker) != expected_fields:
        raise CacheControlError(
            f"ownership marker must contain exactly {sorted(expected_fields)}"
        )
    if marker.get("schema") != OWNER_SCHEMA:
        raise CacheControlError(f"ownership marker schema must be {OWNER_SCHEMA}")
    marker_uid = marker.get("owner_uid")
    if not isinstance(marker_uid, int) or isinstance(marker_uid, bool):
        raise CacheControlError("ownership marker owner_uid must be an integer")
    if marker_uid != root.owner_uid:
        raise CacheControlError("ownership marker owner_uid does not match --root")
    marker_nonce = marker.get("nonce")
    if not isinstance(marker_nonce, str) or not hmac.compare_digest(marker_nonce, nonce):
        raise CacheControlError("ownership marker nonce does not match --nonce")


def load_selection(
    args: argparse.Namespace,
    root: OwnedRoot,
) -> tuple[str, list[str], dict[str, Any]]:
    if args.manifest is not None:
        if args.db_kind is not None or args.file:
            raise CacheControlError(
                "--manifest cannot be combined with --db-kind or --file"
            )
        manifest_path = normalize_relative_path(args.manifest, "--manifest")
        manifest, _ = read_control_json(root, manifest_path, "cache manifest")
        expected_fields = {"schema", "database_kind", "files"}
        if set(manifest) != expected_fields:
            raise CacheControlError(
                f"cache manifest must contain exactly {sorted(expected_fields)}"
            )
        if manifest.get("schema") != MANIFEST_SCHEMA:
            raise CacheControlError(f"cache manifest schema must be {MANIFEST_SCHEMA}")
        database_kind = manifest.get("database_kind")
        raw_files = manifest.get("files")
        if database_kind not in {"sqlite", "postgres"}:
            raise CacheControlError(
                "cache manifest database_kind must be sqlite or postgres"
            )
        if not isinstance(raw_files, list) or not raw_files:
            raise CacheControlError("cache manifest files must be a non-empty array")
        if any(not isinstance(item, str) for item in raw_files):
            raise CacheControlError("cache manifest files must contain only strings")
        source = {"type": "manifest", "manifest": manifest_path}
    else:
        if args.db_kind is None:
            raise CacheControlError("--db-kind is required with --file")
        if not args.file:
            raise CacheControlError("at least one --file is required")
        database_kind = args.db_kind
        raw_files = args.file
        source = {"type": "explicit", "manifest": None}

    if len(raw_files) > MAX_TARGET_FILES:
        raise CacheControlError(f"target list exceeds {MAX_TARGET_FILES} files")
    files = [
        normalize_relative_path(item, f"target file {index}")
        for index, item in enumerate(raw_files, 1)
    ]
    if len(set(files)) != len(files):
        raise CacheControlError("target file list contains duplicates")
    if OWNER_MARKER in files or source["manifest"] in files:
        raise CacheControlError("control files must not be cache targets")
    if database_kind == "postgres":
        invalid = [path for path in files if not POSTGRES_RELATION.fullmatch(path)]
        if invalid:
            raise CacheControlError(
                "PostgreSQL targets must be base/<db_oid>/<numeric_relfilenode> "
                f"relation files; rejected {invalid[0]!r}"
            )
    return database_kind, files, source


def open_targets(root: OwnedRoot, files: Sequence[str]) -> list[Target]:
    targets: list[Target] = []
    try:
        for relative_path in files:
            descriptor = open_beneath(root, relative_path, os.O_RDONLY)
            file_stat = os.fstat(descriptor)
            if not stat.S_ISREG(file_stat.st_mode):
                os.close(descriptor)
                raise CacheControlError(
                    f"target must be a regular file: {relative_path}"
                )
            if file_stat.st_uid != root.owner_uid:
                os.close(descriptor)
                raise CacheControlError(
                    f"target must be owned by effective uid: {relative_path}"
                )
            if file_stat.st_nlink != 1:
                os.close(descriptor)
                raise CacheControlError(
                    f"target must not have hard links: {relative_path}"
                )
            targets.append(Target(relative_path, descriptor, file_stat))
        return targets
    except Exception:
        for target in targets:
            os.close(target.descriptor)
        raise


def stable_identity(file_stat: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
        file_stat.st_ctime_ns,
    )


def page_count(size: int, page_size: int) -> int:
    return math.ceil(size / page_size) if size else 0


def resident_ratio(resident: int | None, pages: int) -> float | None:
    if resident is None or pages == 0:
        return None
    return resident / pages


def cache_state(resident: int | None, pages: int) -> str:
    if resident is None:
        return "measurement_failed"
    if pages == 0:
        return "empty_file"
    if resident == 0:
        return "not_resident"
    if resident == pages:
        return "fully_resident"
    return "partially_resident"


def error_payload(error: BaseException) -> dict[str, Any]:
    code = error.errno if isinstance(error, OSError) else None
    return {
        "type": type(error).__name__,
        "errno": code,
        "message": os.strerror(code) if code is not None else str(error),
    }


def base_file_report(target: Target, page_size: int) -> dict[str, Any]:
    size = target.initial_stat.st_size
    pages = page_count(size, page_size)
    return {
        "relative_path": target.relative_path,
        "bytes": size,
        "pages": pages,
        "resident_pages_before": None,
        "resident_ratio_before": None,
        "cache_state_before": "measurement_pending",
        "resident_pages_after": None,
        "resident_ratio_after": None,
        "cache_state_after": "not_measured",
        "status": "pending",
        "successful": False,
        "flush": {
            "status": "not_requested",
            "dirty_pages_before": None,
            "dirty_observation": "mincore does not expose per-file dirty-page counts",
        },
        "eviction": {"status": "not_requested", "errno": None},
        "error": None,
    }


def set_before(report: dict[str, Any], resident: int) -> None:
    report["resident_pages_before"] = resident
    report["resident_ratio_before"] = resident_ratio(resident, report["pages"])
    report["cache_state_before"] = cache_state(resident, report["pages"])


def set_after(report: dict[str, Any], resident: int) -> None:
    report["resident_pages_after"] = resident
    report["resident_ratio_after"] = resident_ratio(resident, report["pages"])
    report["cache_state_after"] = cache_state(resident, report["pages"])


def inspect_target(cache: LinuxPageCache, target: Target) -> dict[str, Any]:
    report = base_file_report(target, cache.page_size)
    try:
        set_before(
            report,
            cache.resident_pages(target.descriptor, target.initial_stat.st_size),
        )
        final_stat = os.fstat(target.descriptor)
        if stable_identity(final_stat) != stable_identity(target.initial_stat):
            report["status"] = "changed_during_observation"
            return report
        report["status"] = "observed"
        report["successful"] = True
    except (CacheControlError, OSError) as error:
        report["status"] = "measurement_failed"
        report["error"] = error_payload(error)
    return report


def sequential_read(
    descriptor: int,
    size: int,
    chunk_bytes: int,
    calculate_hash: bool,
) -> tuple[int, str | None]:
    os.lseek(descriptor, 0, os.SEEK_SET)
    remaining = size
    read_bytes = 0
    digest = hashlib.sha256() if calculate_hash else None
    while remaining:
        chunk = os.read(descriptor, min(chunk_bytes, remaining))
        if not chunk:
            break
        read_bytes += len(chunk)
        remaining -= len(chunk)
        if digest is not None:
            digest.update(chunk)
    return read_bytes, digest.hexdigest() if digest is not None else None


def warm_target(
    cache: LinuxPageCache,
    target: Target,
    chunk_bytes: int,
    calculate_hash: bool,
) -> dict[str, Any]:
    report = base_file_report(target, cache.page_size)
    report["read"] = {"bytes": 0, "chunk_bytes": chunk_bytes}
    try:
        set_before(
            report,
            cache.resident_pages(target.descriptor, target.initial_stat.st_size),
        )
        read_bytes, digest = sequential_read(
            target.descriptor,
            target.initial_stat.st_size,
            chunk_bytes,
            calculate_hash,
        )
        report["read"]["bytes"] = read_bytes
        if calculate_hash:
            report["sha256"] = digest
        set_after(
            report,
            cache.resident_pages(target.descriptor, target.initial_stat.st_size),
        )
        final_stat = os.fstat(target.descriptor)
        if stable_identity(final_stat) != stable_identity(target.initial_stat):
            report["status"] = "changed_during_warm"
        elif read_bytes != target.initial_stat.st_size:
            report["status"] = "short_read"
        elif report["pages"] == 0:
            report["status"] = "empty_file"
            report["successful"] = True
        elif report["cache_state_after"] == "fully_resident":
            report["status"] = "warmed"
            report["successful"] = True
        elif report["cache_state_after"] == "partially_resident":
            report["status"] = "partially_resident"
        else:
            report["status"] = "not_resident_after_read"
    except (CacheControlError, OSError) as error:
        report["status"] = "warm_failed"
        report["error"] = error_payload(error)
    return report


def evict_target(cache: LinuxPageCache, target: Target) -> dict[str, Any]:
    report = base_file_report(target, cache.page_size)
    report["flush"]["status"] = "pending"
    report["eviction"]["status"] = "pending"
    try:
        set_before(
            report,
            cache.resident_pages(target.descriptor, target.initial_stat.st_size),
        )
        try:
            os.fsync(target.descriptor)
            report["flush"]["status"] = "completed"
            report["flush"]["result"] = (
                "file data and metadata were synchronized before fadvise; "
                "dirty pages before fsync were not observable"
            )
        except OSError as error:
            report["flush"]["status"] = "failed"
            report["flush"]["error"] = error_payload(error)
            report["eviction"]["status"] = "skipped_after_flush_failure"
            report["status"] = "flush_failed"
            try:
                set_after(
                    report,
                    cache.resident_pages(
                        target.descriptor,
                        target.initial_stat.st_size,
                    ),
                )
            except OSError as measurement_error:
                report["error"] = error_payload(measurement_error)
            return report

        advice_error = cache.evict(target.descriptor)
        if advice_error:
            report["eviction"] = {
                "status": "failed",
                "errno": advice_error,
                "message": os.strerror(advice_error),
            }
        else:
            report["eviction"] = {"status": "advised", "errno": None}

        set_after(
            report,
            cache.resident_pages(target.descriptor, target.initial_stat.st_size),
        )
        final_stat = os.fstat(target.descriptor)
        if stable_identity(final_stat) != stable_identity(target.initial_stat):
            report["status"] = "changed_during_evict"
        elif advice_error:
            report["status"] = "eviction_failed"
        elif report["pages"] == 0:
            report["status"] = "empty_file"
            report["successful"] = True
        elif report["resident_pages_after"] == 0:
            report["status"] = "evicted"
            report["successful"] = True
        elif report["cache_state_after"] == "partially_resident":
            report["status"] = "partially_resident"
        else:
            report["status"] = "fully_resident_after_evict"
    except (CacheControlError, OSError) as error:
        report["status"] = "evict_failed"
        report["error"] = error_payload(error)
    return report


def aggregate_state(
    reports: Sequence[dict[str, Any]],
    suffix: str,
) -> tuple[int, int | None, float | None, str]:
    resident_key = f"resident_pages_{suffix}"
    measured = [report for report in reports if report[resident_key] is not None]
    measured_pages = sum(report["pages"] for report in measured)
    if not measured:
        return 0, None, None, "not_measured"
    resident_pages = sum(report[resident_key] for report in measured)
    if len(measured) != len(reports):
        state = "measurement_incomplete"
    else:
        state = cache_state(resident_pages, measured_pages)
    return (
        measured_pages,
        resident_pages,
        resident_ratio(resident_pages, measured_pages),
        state,
    )


def utc_from_ns(timestamp_ns: int) -> str:
    seconds, nanoseconds = divmod(timestamp_ns, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{prefix}.{nanoseconds:09d}Z"


def build_report(
    args: argparse.Namespace,
    root: OwnedRoot,
    database_kind: str,
    source: dict[str, Any],
    file_reports: Sequence[dict[str, Any]],
    page_size: int,
    started_wall_ns: int,
    started_monotonic_ns: int,
    ended_monotonic_ns: int,
) -> dict[str, Any]:
    operation = args.command
    successful = all(report["successful"] for report in file_reports)
    if operation == "inspect":
        status = "observed" if successful else "incomplete"
    elif operation == "warm":
        status = "warmed" if successful else "incomplete"
    else:
        status = "evicted" if successful else "incomplete"
    before_pages, before_resident, before_ratio, before_state = aggregate_state(
        file_reports,
        "before",
    )
    after_pages, after_resident, after_ratio, after_state = aggregate_state(
        file_reports,
        "after",
    )
    elapsed_ns = ended_monotonic_ns - started_monotonic_ns
    ended_wall_ns = started_wall_ns + elapsed_ns
    return {
        "schema": REPORT_SCHEMA,
        "operation": operation,
        "status": status,
        "successful": successful,
        "database_kind": database_kind,
        "started_at_utc": utc_from_ns(started_wall_ns),
        "ended_at_utc": utc_from_ns(ended_wall_ns),
        "started_monotonic_ns": started_monotonic_ns,
        "ended_monotonic_ns": ended_monotonic_ns,
        "duration_ns": elapsed_ns,
        "clock": {
            "method": "CLOCK_REALTIME start plus CLOCK_MONOTONIC elapsed",
            "wall_clock_changes_during_operation": "excluded from elapsed time",
        },
        "root": {
            "path": str(root.path),
            "device": root.device,
            "owner_uid": root.owner_uid,
            "marker": OWNER_MARKER,
            "nonce_verified": True,
        },
        "selection": {
            **source,
            "file_count": len(file_reports),
        },
        "preconditions": {
            "files_quiesced_acknowledged": bool(
                getattr(args, "files_quiesced", False)
            ),
            "tool_verified_quiescence": False,
            "required_for_operation": operation in {"warm", "evict"},
            "flush_behavior": (
                "fsync each target before POSIX_FADV_DONTNEED"
                if operation == "evict"
                else "no file flush performed"
            ),
        },
        "page_accounting": {
            "page_size_bytes": page_size,
            "file_pages": "ceil(file bytes / page size)",
            "partial_final_page": "counts as one page and is never rounded down",
            "mincore_mapping": "PROT_NONE MAP_SHARED in bounded windows",
        },
        "totals": {
            "file_count": len(file_reports),
            "bytes": sum(report["bytes"] for report in file_reports),
            "pages": sum(report["pages"] for report in file_reports),
            "measured_pages_before": before_pages,
            "resident_pages_before": before_resident,
            "resident_ratio_before": before_ratio,
            "cache_state_before": before_state,
            "measured_pages_after": after_pages,
            "resident_pages_after": after_resident,
            "resident_ratio_after": after_ratio,
            "cache_state_after": after_state,
        },
        "files": list(file_reports),
        "limitations": [
            "This report measures Linux filesystem page-cache residency for only the explicitly listed files.",
            "The first inspection reports measured residency; it is never labeled cold merely because it was first.",
            "mincore reports residency, not per-file dirty-page counts or the cause of residency.",
            "SQLite WAL, rollback-journal, and shared-memory files are excluded unless explicitly listed; this tool does not checkpoint SQLite.",
            "PostgreSQL targets are limited to base/<db_oid>/<numeric_relfilenode> relation files; metadata, WAL, temporary files, tablespaces, and other forks are excluded unless they match and are explicitly listed.",
            "This tool neither clears nor proves PostgreSQL shared_buffers, database process caches, storage-controller caches, or remote caches. Main must stop and separately restart the database when shared_buffers control is required.",
            "The quiescence flag records an operator assertion; this tool cannot prove that every writer stopped.",
            "No global host or virtual-machine cache control is used.",
        ],
    }


def reserve_report_output(raw_output: str | None, root: OwnedRoot) -> int | None:
    output = required_text(raw_output, "--output")
    if output == "-":
        return None
    output_path = Path(output).expanduser()
    parent = output_path.parent.resolve(strict=True)
    candidate = parent / output_path.name
    if candidate == root.path or root.path in candidate.parents:
        raise CacheControlError("--output must be outside --root")
    return os.open(
        candidate,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )


def write_report(descriptor: int | None, report: dict[str, Any]) -> None:
    encoded = (
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    ).encode("utf-8")
    if descriptor is None:
        sys.stdout.buffer.write(encoded)
        sys.stdout.buffer.flush()
        return
    offset = 0
    while offset < len(encoded):
        offset += os.write(descriptor, encoded[offset:])
    os.fsync(descriptor)


def run(args: argparse.Namespace) -> int:
    if args.command == "inspect" and args.sha256:
        raise CacheControlError(
            "--sha256 is not allowed with inspect because hashing would warm target pages"
        )
    if args.command == "evict" and args.sha256:
        raise CacheControlError(
            "--sha256 is not allowed with evict because hashing would re-read target pages"
        )
    if args.command in {"warm", "evict"} and not args.files_quiesced:
        raise CacheControlError(
            f"{args.command} requires --files-quiesced after Main stops writers and completes database-specific flush/checkpoint work"
        )
    if not 4_096 <= args.read_chunk_bytes <= MAX_READ_CHUNK_BYTES:
        raise CacheControlError(
            f"--read-chunk-bytes must be between 4096 and {MAX_READ_CHUNK_BYTES}"
        )

    root = open_root(args.root)
    targets: list[Target] = []
    output_descriptor: int | None = None
    try:
        verify_owner_marker(root, args.nonce)
        database_kind, files, source = load_selection(args, root)
        targets = open_targets(root, files)
        cache = LinuxPageCache()
        output_descriptor = reserve_report_output(args.output, root)
        started_wall_ns = time.time_ns()
        started_monotonic_ns = time.monotonic_ns()
        if args.command == "inspect":
            file_reports = [inspect_target(cache, target) for target in targets]
        elif args.command == "warm":
            file_reports = [
                warm_target(
                    cache,
                    target,
                    args.read_chunk_bytes,
                    args.sha256,
                )
                for target in targets
            ]
        else:
            file_reports = [evict_target(cache, target) for target in targets]
        ended_monotonic_ns = time.monotonic_ns()
        report = build_report(
            args,
            root,
            database_kind,
            source,
            file_reports,
            cache.page_size,
            started_wall_ns,
            started_monotonic_ns,
            ended_monotonic_ns,
        )
        write_report(output_descriptor, report)
        return 0 if report["successful"] else 3
    finally:
        if output_descriptor is not None:
            os.close(output_descriptor)
        for target in targets:
            os.close(target.descriptor)
        os.close(root.descriptor)


def add_common_arguments(command: argparse.ArgumentParser) -> None:
    command.add_argument(
        "--root",
        required=True,
        help="owned Linux named-volume root; symlinks and filesystem root are rejected",
    )
    command.add_argument(
        "--nonce",
        required=True,
        help=f"nonce matching {OWNER_MARKER}; the nonce is verified but never reported",
    )
    selection = command.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--manifest",
        help="normalized manifest path relative to --root",
    )
    selection.add_argument(
        "--file",
        action="append",
        help="normalized target path relative to --root; repeat for each file",
    )
    command.add_argument(
        "--db-kind",
        choices=("sqlite", "postgres"),
        help="required with --file; the manifest supplies its own database_kind",
    )
    command.add_argument(
        "--output",
        default="-",
        help="JSON destination outside --root; default '-' writes stdout and files are never overwritten",
    )
    command.add_argument(
        "--sha256",
        action="store_true",
        help="include hashes during warm's existing sequential read; rejected for inspect and evict",
    )
    command.add_argument(
        "--read-chunk-bytes",
        type=int,
        default=DEFAULT_READ_CHUNK_BYTES,
        help=f"bounded warm read size, 4096..{MAX_READ_CHUNK_BYTES} (default {DEFAULT_READ_CHUNK_BYTES})",
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Inspect, warm, or evict Linux filesystem page-cache pages for explicit "
            "files in one owned QA volume. This is not a database-server cache tool."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""Control files created by Main, never by this tool:
  {OWNER_MARKER}
    {{"schema":"{OWNER_SCHEMA}","nonce":"<16-256 chars>","owner_uid":<effective uid>}}

  manifest.json
    {{"schema":"{MANIFEST_SCHEMA}","database_kind":"sqlite|postgres","files":["relative/path"]}}

Safety and lifecycle:
  inspect maps targets PROT_NONE and calls mincore without reading file data.
  warm requires --files-quiesced, reads each byte in bounded chunks, then calls mincore.
  evict requires --files-quiesced, fsyncs each file, calls POSIX_FADV_DONTNEED,
  and reports the actual mincore result. Partial or failed eviction exits 3 and
  remains explicit in JSON. Guard/argument errors exit 2.

Before warm or evict, Main must stop every writer. For SQLite, Main also owns any
required checkpoint and must explicitly list the database, -wal, -shm, or journal
files whose filesystem pages matter. For PostgreSQL, Main must stop the server,
complete database-specific flush work, list only base/<db_oid>/<relfilenode>
files, and restart separately when shared_buffers must be cleared. This tool
requires normal read/fsync access to same-uid files; it needs no global cache
privilege and does not alter host-wide or VM-wide cache state.
""",
    )
    commands = result.add_subparsers(dest="command", required=True)

    inspect_parser = commands.add_parser(
        "inspect",
        help="measure current residency without reading target data",
    )
    add_common_arguments(inspect_parser)

    warm_parser = commands.add_parser(
        "warm",
        help="sequentially read quiesced targets and verify residency",
    )
    add_common_arguments(warm_parser)
    warm_parser.add_argument(
        "--files-quiesced",
        action="store_true",
        help="assert that Main stopped writers and completed required database flush work",
    )

    evict_parser = commands.add_parser(
        "evict",
        help="fsync and advise away quiesced target pages, then verify residency",
    )
    add_common_arguments(evict_parser)
    evict_parser.add_argument(
        "--files-quiesced",
        action="store_true",
        help="assert that Main stopped writers and completed required database flush work",
    )
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        return run(args)
    except (CacheControlError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
