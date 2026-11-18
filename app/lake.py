"""Core data-lake logic: uploads, consistent reads, background compaction.

Consistency model (single process, SQLite as the source of truth):

* Upload: parquet is written to a tmp file, fsynced, atomically renamed,
  then registered ``active`` in SQLite.  A read only ever sees fully
  registered files.
* Read: the active file list is snapshotted and refcounted under one lock,
  so a read always corresponds to the complete pre- or post-switch state --
  never a mix.  Retired files stay on disk until their refcount hits zero.
* Compaction: merges the files registered at job start into one parquet,
  then switches the registry in a single transaction.  Rows appended during
  the merge are not part of the input snapshot, so they survive the switch
  untouched and remain readable throughout.
* Provenance: every row carries the filename and row count of the upload it
  came from.  The columns live inside the parquet data, so they are carried
  through compaction and remain valid after the original upload's file has
  been reclaimed.  Legacy files written before those columns existed fall
  back to their own filename (uploads) or to "unknown" (compacted files).
* Recovery: the switch and the job-status update commit atomically, so a
  job still ``running`` at startup is guaranteed not to have switched; it
  is simply re-executed.  Orphaned bytes (failed uploads, superseded
  compaction outputs, tmp fragments) are swept before serving traffic.
"""

from __future__ import annotations

import json
import logging
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from .csvio import (
    CANONICAL_SCHEMA,
    LAKE_SCHEMA,
    CsvError,
    empty_lake_table,
    empty_table,
    parse_csv,
)
from .db import connect, init_schema
from .storage import write_parquet_atomic

log = logging.getLogger("buoy-lake")


class ConflictError(RuntimeError):
    """A compaction is already running for this day."""


class NotFoundError(KeyError):
    """Requested object does not exist."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _with_provenance(table: pa.Table, file_name: str) -> pa.Table:
    """Return ``table`` (canonical 3 columns) plus per-row provenance columns."""
    base = table.select(CANONICAL_SCHEMA.names)
    n = base.num_rows
    return base.append_column(
        "source_file", pa.array([file_name] * n, type=pa.string())
    ).append_column(
        "source_row", pa.array(list(range(1, n + 1)), type=pa.int64())
    ).append_column(
        "source_rows", pa.array([n] * n, type=pa.int64())
    )


def _concat_data(tables: list[pa.Table]) -> pa.Table:
    """Concatenate tables using only the canonical data columns.

    ``GET /records`` keeps its original three-column contract even when the
    snapshot mixes legacy (3-column) and current (6-column) files.
    """
    data = [t.select(CANONICAL_SCHEMA.names) for t in tables]
    return pa.concat_tables(data, promote_options="none") if data else empty_table()


def _concat_full(tables: list[pa.Table]) -> pa.Table:
    """Concatenate tables including provenance (all are normalised upstream)."""
    return pa.concat_tables(tables, promote_options="none") if tables else empty_lake_table()


class Lake:
    def __init__(self, data_dir: str | os.PathLike, compaction_delay_sec: float = 0.0):
        self.data_dir = Path(data_dir)
        self.db_path = self.data_dir / "lake_meta.db"
        # Test/demo hook: pause after the input snapshot so callers can
        # observe uploads and reads happening *while* a compaction runs.
        self.compaction_delay_sec = compaction_delay_sec
        # Guards the inflight refcounts and serialises
        # (snapshot + acquire) against (retired-file reclamation).
        self._state_lock = threading.Lock()
        self._inflight: dict[str, int] = {}
        self.data_dir.mkdir(parents=True, exist_ok=True)
        init_schema(str(self.db_path))

    # ------------------------------------------------------------------
    # paths
    # ------------------------------------------------------------------
    def _day_dir(self, day: str) -> Path:
        return self.data_dir / f"dt={day}"

    # ------------------------------------------------------------------
    # uploads
    # ------------------------------------------------------------------
    def upload(self, day: str, content: bytes) -> dict:
        """Parse CSV, write one parquet file, register it. Raises CsvError."""
        table = parse_csv(content)
        n = table.num_rows
        day_dir = self._day_dir(day)
        day_dir.mkdir(parents=True, exist_ok=True)
        final = day_dir / f"part-{uuid4().hex}.parquet"
        # The upload's filename is its batch id: it is returned to the caller
        # and stamped on every row so the row can still be traced back to this
        # upload after compaction has reclaimed the original file.
        table = _with_provenance(table, file_name=final.name)
        write_parquet_atomic(table, final)
        try:
            with connect(str(self.db_path)) as conn:
                conn.execute(
                    "INSERT INTO files(day, path, kind, state, rows, created_at)"
                    " VALUES (?, ?, 'upload', 'active', ?, ?)",
                    (day, str(final), n, _now_iso()),
                )
        except Exception:
            final.unlink(missing_ok=True)  # never registered -> remove bytes
            raise
        log.info("uploaded %s rows to %s", n, final.name)
        return {
            "day": day,
            "file": final.name,
            "rows": n,
            "file_count": self.file_count(day),
        }

    # ------------------------------------------------------------------
    # reading parquet + provenance
    # ------------------------------------------------------------------
    def _snapshot_active(self, day: str) -> list[str]:
        """Active files for ``day`` + inflight ref, atomic w.r.t. the reclaimer."""
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    "SELECT path FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                    (day,),
                ).fetchall()
            paths = [r["path"] for r in rows]
            for p in paths:
                self._inflight[p] = self._inflight.get(p, 0) + 1
            return paths

    def _release(self, paths: list[str]) -> None:
        with self._state_lock:
            for p in paths:
                remaining = self._inflight.get(p, 0) - 1
                if remaining <= 0:
                    self._inflight.pop(p, None)
                else:
                    self._inflight[p] = remaining
        self.collect_garbage()

    def _kinds_for(self, paths: list[str]) -> dict[str, str]:
        """Registered kind for each path (``upload`` / ``compacted``)."""
        kinds: dict[str, str] = {}
        # Chunk the IN-list so days with very many files cannot hit SQLite's
        # bound-parameter limit.
        for start in range(0, len(paths), 500):
            chunk = paths[start : start + 500]
            placeholders = ",".join("?" for _ in chunk)
            with connect(str(self.db_path)) as conn:
                rows = conn.execute(
                    f"SELECT path, kind FROM files WHERE path IN ({placeholders})", chunk
                ).fetchall()
            kinds.update({r["path"]: r["kind"] for r in rows})
        return kinds

    def _read_with_provenance(self, path: str, kind: str | None) -> pa.Table:
        """Read one parquet file, materialising provenance for legacy files.

        Files written by this version already carry ``source_file`` /
        ``source_row`` / ``source_rows``.  Legacy files do not (they may also
        have been produced mid-migration with only some of the columns):

        * a legacy *upload* file is attributed to its own filename;
        * a legacy *compacted* file has no traceable origin, so its rows are
          marked unknown (``null``) -- never the compaction output's name.

        Columns a file does carry are always respected.
        """
        table = pq.read_table(path)
        base = table.select(CANONICAL_SCHEMA.names)
        n = base.num_rows
        names = set(table.column_names)

        if "source_file" in names:
            files = table.column("source_file").cast(pa.string(), safe=False)
        elif kind == "upload" or (kind is None and Path(path).name.startswith("part-")):
            files = pa.array([Path(path).name] * n, type=pa.string())
        else:
            # Unknown origin: no filename may be claimed for these rows.
            files = pa.array([None] * n, type=pa.string())

        if "source_row" in names:
            row_numbers = table.column("source_row").cast(pa.int64(), safe=False)
        elif kind == "upload" or (kind is None and Path(path).name.startswith("part-")):
            row_numbers = pa.array(list(range(1, n + 1)), type=pa.int64())
        else:
            row_numbers = pa.array([None] * n, type=pa.int64())

        if "source_rows" in names:
            row_counts = table.column("source_rows").cast(pa.int64(), safe=False)
        else:
            # The upload's own row count is a valid fallback for a legacy
            # upload file (the whole file was one upload); unknown otherwise.
            if kind == "upload" or (kind is None and Path(path).name.startswith("part-")):
                row_counts = pa.array([n] * n, type=pa.int64())
            else:
                row_counts = pa.array([None] * n, type=pa.int64())

        return (
            base.append_column("source_file", files)
            .append_column("source_row", row_numbers)
            .append_column("source_rows", row_counts)
            .select(LAKE_SCHEMA.names)
            .replace_schema_metadata(None)
        )

    def _read_snapshot(self, day: str) -> tuple[list[str], list[pa.Table]]:
        """Pin the active file set and read all of it (caller releases paths)."""
        paths = self._snapshot_active(day)
        try:
            kinds = self._kinds_for(paths)
            tables = [self._read_with_provenance(p, kinds.get(p)) for p in paths]
        except Exception:
            self._release(paths)
            raise
        return paths, tables

    def read_day(self, day: str) -> dict:
        """All records for the day from one consistent (pre/post-switch) snapshot."""
        paths, tables = self._read_snapshot(day)
        try:
            merged = _concat_data(tables)
            cols = merged.to_pydict()
            records = [
                {"buoy_id": b, "ts_utc": _iso_z(t), "salinity": s}
                for b, t, s in zip(cols["buoy_id"], cols["ts_utc"], cols["salinity"])
            ]
            return {
                "day": day,
                "file_count": len(paths),
                "row_count": merged.num_rows,
                "records": records,
            }
        finally:
            self._release(paths)

    def conflicts(self, day: str) -> dict:
        """Finite readings whose salinity disagrees for one buoy + UTC instant.

        A conflict is a ``(buoy_id, ts_utc)`` pair carrying at least two
        *distinct finite* salinity values.  Identical values (a repeated row,
        or the same value uploaded twice) are **not** conflicts; they are
        counted together on the reading instead of being listed separately.

        Each conflicting reading reports every upload batch it appears in,
        as the filename returned by ``POST /days/{day}/upload`` plus the
        1-based row number(s) inside that upload and the upload's row count.
        Those identifiers live inside the parquet data, so they remain valid
        once compaction has reclaimed the original upload file.  Legacy rows
        whose origin cannot be established report ``source_file: null``
        (``source_known: false``) -- never the compaction output's name.
        """
        paths, tables = self._read_snapshot(day)
        try:
            merged = _concat_full(tables)
            cols = merged.to_pydict()
        finally:
            self._release(paths)

        # (buoy, instant) -> salinity -> {"count": n, "sources": {file: rows}}
        groups: dict[tuple[str, datetime], dict[float, dict]] = {}
        for buoy, ts, sal, src, row_no, src_rows in zip(
            cols["buoy_id"],
            cols["ts_utc"],
            cols["salinity"],
            cols["source_file"],
            cols["source_row"],
            cols["source_rows"],
        ):
            if buoy is None or ts is None or sal is None or not math.isfinite(sal):
                continue  # only complete, finite readings can conflict
            entry = groups.setdefault((buoy, ts), {}).setdefault(
                sal, {"count": 0, "sources": {}}
            )
            entry["count"] += 1
            source = entry["sources"].setdefault(
                src, {"file": src, "row_numbers": [], "row_count": src_rows}
            )
            if row_no is not None:
                source["row_numbers"].append(int(row_no))

        conflicts = []
        flat: list[dict] = []
        for (buoy, ts), by_value in sorted(
            groups.items(), key=lambda kv: (kv[0][0], kv[0][1])
        ):
            if len(by_value) < 2:
                continue  # one distinct value -> duplicates, not a conflict
            ts_iso = _iso_z(ts)
            readings = []
            for sal in sorted(by_value):
                entry = by_value[sal]
                sources = [
                    {
                        "file": s["file"],
                        "source_file": s["file"],
                        "filename": s["file"],
                        "row_numbers": sorted(s["row_numbers"]),
                        "line_numbers": sorted(s["row_numbers"]),
                        "count": len(s["row_numbers"]),
                        "row_count": s["row_count"],
                        "rows": s["row_count"],
                        "source_known": s["file"] is not None,
                    }
                    for s in sorted(
                        entry["sources"].values(),
                        key=lambda s: (s["file"] is None, s["file"] or ""),
                    )
                ]
                # Flat summary of the primary source, for easy consumption.
                primary = sources[0]
                reading = {
                    "salinity": sal,
                    "count": entry["count"],
                    "file": primary["file"],
                    "source_file": primary["file"],
                    "filename": primary["file"],
                    "row_numbers": list(primary["row_numbers"]),
                    "line_numbers": list(primary["row_numbers"]),
                    "row_count": primary["row_count"],
                    "rows": primary["row_count"],
                    "source_known": primary["source_known"],
                    # Explicit marker for untraceable legacy rows.
                    "source": "known" if primary["source_known"] else "unknown",
                    "sources": sources,
                }
                readings.append(reading)
                flat.append({"buoy_id": buoy, "ts_utc": ts_iso, **reading})

            # Distinct uploads involved in this conflict, in stable order.
            files: dict[str, dict] = {}
            for reading in readings:
                for source in reading["sources"]:
                    key = source["file"]
                    entry = files.setdefault(
                        key,
                        {
                            "file": key,
                            "source_file": key,
                            "row_numbers": [],
                            "row_count": source["row_count"],
                            "source_known": source["source_known"],
                        },
                    )
                    entry["row_numbers"].extend(source["row_numbers"])
            for entry in files.values():
                entry["row_numbers"] = sorted(set(entry["row_numbers"]))
            file_list = sorted(
                files.values(), key=lambda f: (f["file"] is None, f["file"] or "")
            )

            conflicts.append(
                {
                    "buoy_id": buoy,
                    "ts_utc": ts_iso,
                    "salinity_values": sorted(by_value),
                    "salinities": sorted(by_value),
                    "readings": readings,
                    "files": file_list,
                }
            )

        return {
            "day": day,
            "conflict_count": len(conflicts),
            "file_count": len(paths),
            # Grouped by (buoy, instant); each entry lists the distinct
            # salinity values with their source uploads.
            "conflicts": conflicts,
            # The same information flattened: one item per distinct reading.
            "readings": flat,
        }

    def file_count(self, day: str) -> int:
        with connect(str(self.db_path)) as conn:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM files WHERE day = ? AND state = 'active'", (day,)
            ).fetchone()
        return n

    def list_files(self, day: str) -> dict:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, kind, rows, created_at FROM files"
                " WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
        return {
            "day": day,
            "file_count": len(rows),
            "files": [
                {
                    "file": Path(r["path"]).name,
                    "kind": r["kind"],
                    "rows": r["rows"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
        }

    # ------------------------------------------------------------------
    # compaction
    # ------------------------------------------------------------------
    def start_compaction(self, day: str) -> dict:
        """Register and launch a background compaction. Raises ConflictError."""
        input_rows = 0
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT path, rows FROM files WHERE day = ? AND state = 'active' ORDER BY id",
                (day,),
            ).fetchall()
            input_paths = [r["path"] for r in rows]
            input_rows = sum(r["rows"] for r in rows)
            try:
                cur = conn.execute(
                    "INSERT INTO compactions(day, status, input_paths, input_rows, created_at)"
                    " VALUES (?, 'running', ?, ?, ?)",
                    (day, json.dumps(input_paths), input_rows, _now_iso()),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError(f"a compaction is already running for day {day}") from exc
            job_id = cur.lastrowid
        self._launch_worker(job_id, day, input_paths)
        log.info("compaction job %s started for %s over %d files", job_id, day, len(input_paths))
        return self.get_compaction(job_id)

    def _launch_worker(self, job_id: int, day: str, input_paths: list[str]) -> None:
        t = threading.Thread(
            target=self._run_compaction,
            args=(job_id, day, input_paths),
            name=f"compact-{day}-{job_id}",
            daemon=True,
        )
        t.start()

    def _run_compaction(self, job_id: int, day: str, input_paths: list[str]) -> None:
        output_path: Path | None = None
        try:
            if self.compaction_delay_sec > 0:
                time.sleep(self.compaction_delay_sec)

            if input_paths:
                # Normalise legacy inputs first: an old upload's rows adopt
                # its filename, an old compaction output's rows stay
                # untraceable (null source).  The merged output then carries
                # one row of provenance per row, independent of the original
                # bytes -- so reclaiming the inputs loses nothing.
                kinds = self._kinds_for(input_paths)
                tables = [
                    self._read_with_provenance(p, kinds.get(p)) for p in input_paths
                ]
                merged = pa.concat_tables(tables, promote_options="none")
            else:
                merged = empty_lake_table()

            if input_paths:
                day_dir = self._day_dir(day)
                day_dir.mkdir(parents=True, exist_ok=True)
                output_path = day_dir / f"compacted-{job_id}-{uuid4().hex[:8]}.parquet"
                # Fully written + fsynced *before* the registry switch.
                write_parquet_atomic(merged, output_path)

            # Atomic switch: retire inputs, register output, mark job done.
            conn = connect(str(self.db_path))
            try:
                conn.execute("BEGIN IMMEDIATE")
                if input_paths:
                    placeholders = ",".join("?" for _ in input_paths)
                    conn.execute(
                        f"UPDATE files SET state = 'retired'"
                        f" WHERE day = ? AND state = 'active' AND path IN ({placeholders})",
                        [day, *input_paths],
                    )
                    conn.execute(
                        "INSERT INTO files(day, path, kind, state, rows, created_at)"
                        " VALUES (?, ?, 'compacted', 'active', ?, ?)",
                        (day, str(output_path), merged.num_rows, _now_iso()),
                    )
                conn.execute(
                    "UPDATE compactions SET status = 'done', output_path = ?,"
                    " output_rows = ?, finished_at = ? WHERE id = ?",
                    (str(output_path) if output_path else None, merged.num_rows, _now_iso(), job_id),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
            log.info("compaction job %s done: %d files -> %d rows", job_id, len(input_paths), merged.num_rows)
        except Exception as exc:  # originals stay registered; nothing is lost
            log.exception("compaction job %s failed", job_id)
            if output_path is not None:
                output_path.unlink(missing_ok=True)
            try:
                with connect(str(self.db_path)) as conn:
                    conn.execute(
                        "UPDATE compactions SET status = 'failed', error = ?, finished_at = ?"
                        " WHERE id = ? AND status = 'running'",
                        (f"{type(exc).__name__}: {exc}", _now_iso(), job_id),
                    )
            except Exception:
                log.exception("could not mark compaction job %s failed", job_id)
        finally:
            try:
                self.collect_garbage()
            except Exception:
                log.exception("garbage collection after job %s failed", job_id)

    # ------------------------------------------------------------------
    # compaction status
    # ------------------------------------------------------------------
    @staticmethod
    def _job_dict(row: sqlite3.Row) -> dict:
        return {
            "job_id": row["id"],
            "day": row["day"],
            "status": row["status"],
            "input_files": [Path(p).name for p in json.loads(row["input_paths"])],
            "input_rows": row["input_rows"],
            "output_file": Path(row["output_path"]).name if row["output_path"] else None,
            "output_rows": row["output_rows"],
            "error": row["error"],
            "created_at": row["created_at"],
            "finished_at": row["finished_at"],
        }

    def get_compaction(self, job_id: int) -> dict:
        with connect(str(self.db_path)) as conn:
            row = conn.execute("SELECT * FROM compactions WHERE id = ?", (job_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"compaction job {job_id} not found")
        return self._job_dict(row)

    def list_compactions(self, day: str) -> list[dict]:
        with connect(str(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT * FROM compactions WHERE day = ? ORDER BY id DESC", (day,)
            ).fetchall()
        return [self._job_dict(r) for r in rows]

    # ------------------------------------------------------------------
    # garbage collection of retired files
    # ------------------------------------------------------------------
    def collect_garbage(self) -> int:
        """Unlink retired files whose in-flight readers have all finished.

        Never touches ``active`` files, so freshly appended parquet files are
        always safe.
        """
        collected = 0
        with self._state_lock:
            with connect(str(self.db_path)) as conn:
                retired = conn.execute(
                    "SELECT id, path FROM files WHERE state = 'retired'"
                ).fetchall()
                for row in retired:
                    if self._inflight.get(row["path"], 0) > 0:
                        continue  # a reader is still using these bytes
                    try:
                        os.unlink(row["path"])
                    except FileNotFoundError:
                        pass
                    except OSError:
                        log.exception("could not unlink retired file %s", row["path"])
                        continue  # keep the row, retry on the next sweep
                    conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                    collected += 1
        if collected:
            log.info("collected %d retired file(s)", collected)
        return collected

    # ------------------------------------------------------------------
    # crash recovery (run once at startup, before serving traffic)
    # ------------------------------------------------------------------
    def recover(self) -> dict:
        """Reconcile disk and registry, then resume interrupted compactions."""
        stats = {"retired_removed": 0, "orphans_removed": 0, "jobs_resumed": 0}

        with connect(str(self.db_path)) as conn:
            # 1. No reader can be in flight right after boot -> retired files
            #    are safe to remove immediately.
            retired = conn.execute("SELECT id, path FROM files WHERE state = 'retired'").fetchall()
            for row in retired:
                Path(row["path"]).unlink(missing_ok=True)
                conn.execute("DELETE FROM files WHERE id = ?", (row["id"],))
                stats["retired_removed"] += 1
            known = {r["path"] for r in conn.execute("SELECT path FROM files").fetchall()}
            running = conn.execute(
                "SELECT id, day, input_paths FROM compactions WHERE status = 'running'"
            ).fetchall()

        # 2. Sweep tmp fragments and parquet files the registry does not know
        #    (interrupted uploads, superseded compaction outputs).
        for day_dir in sorted(self.data_dir.glob("dt=*")):
            if not day_dir.is_dir():
                continue
            for f in day_dir.iterdir():
                if not f.is_file():
                    continue
                if ".tmp" in f.name or (f.suffix == ".parquet" and str(f) not in known):
                    f.unlink(missing_ok=True)
                    stats["orphans_removed"] += 1

        # 3. A job still 'running' cannot have switched the registry (switch
        #    and status update are one transaction), so its inputs are all
        #    still active: simply run it again.
        for job in running:
            input_paths = json.loads(job["input_paths"])
            log.warning("resuming interrupted compaction job %s for %s", job["id"], job["day"])
            self._launch_worker(job["id"], job["day"], input_paths)
            stats["jobs_resumed"] += 1

        log.info("recovery finished: %s", stats)
        return stats
