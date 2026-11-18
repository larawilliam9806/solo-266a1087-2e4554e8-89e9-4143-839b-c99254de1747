"""End-to-end tests for the buoy data-lake compaction service."""

from __future__ import annotations

import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa  # noqa: E402

from app.csvio import CANONICAL_SCHEMA  # noqa: E402
from app.db import connect  # noqa: E402
from app.lake import Lake  # noqa: E402
from app.main import create_app  # noqa: E402

DAY = "2026-11-18"

CSV_A = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:01:00Z,35.1\n"
    "B002,2026-11-18T00:02:00Z,35.2\n"
)
CSV_B = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T01:01:00Z,35.3\n"
    "B003,2026-11-18T01:02:00Z,34.9\n"
)
CSV_C = "buoy_id,ts_utc,salinity\nB004,2026-11-18T02:00:00Z,36.0\n"
# duplicate rows on purpose: compaction must keep every occurrence
CSV_DUP = (
    "buoy_id,ts_utc,salinity\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
    "B009,2026-11-18T03:00:00Z,33.3\n"
)


def upload(client: TestClient, csv_text: str, day: str = DAY, raw: bool = False):
    if raw:
        r = client.post(f"/days/{day}/upload", content=csv_text.encode(),
                        headers={"Content-Type": "text/csv"})
    else:
        r = client.post(f"/days/{day}/upload",
                        files={"file": ("obs.csv", csv_text.encode(), "text/csv")})
    assert r.status_code == 200, r.text
    return r.json()


def wait_job(client: TestClient, day: str, job_id: int, timeout: float = 15.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/days/{day}/compactions/{job_id}")
        assert r.status_code == 200, r.text
        job = r.json()
        if job["status"] in ("done", "failed"):
            return job
        time.sleep(0.05)
    pytest.fail(f"compaction job {job_id} did not finish in {timeout}s")


def make_client(tmp_path, **kwargs) -> TestClient:
    return TestClient(create_app(data_dir=str(tmp_path / "data"), **kwargs))


# ---------------------------------------------------------------------------
# basic upload / read
# ---------------------------------------------------------------------------

def test_upload_and_read_roundtrip(tmp_path):
    with make_client(tmp_path) as c:
        up1 = upload(c, CSV_A)                      # multipart
        assert up1["rows"] == 2 and up1["file_count"] == 1
        up2 = upload(c, CSV_B, raw=True)            # raw text/csv body
        assert up2["rows"] == 2 and up2["file_count"] == 2

        r = c.get(f"/days/{DAY}/records")
        assert r.status_code == 200
        body = r.json()
        assert body["file_count"] == 2
        assert body["row_count"] == 4
        got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in body["records"]}
        assert got == {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
            ("B001", "2026-11-18T01:01:00Z", 35.3),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
        }

        r = c.get(f"/days/{DAY}/files")
        assert r.status_code == 200
        assert r.json()["file_count"] == 2


def test_headerless_csv_and_naive_timestamps(tmp_path):
    csv_text = "B007,2026-11-18 05:06:07,35.55\n"
    with make_client(tmp_path) as c:
        upload(c, csv_text, raw=True)
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 1
        assert body["records"][0]["ts_utc"] == "2026-11-18T05:06:07Z"
        assert body["records"][0]["salinity"] == 35.55


def test_invalid_csv_rejected(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/days/{DAY}/upload", content=b"buoy_id,ts_utc,salinity\nB1,not-a-time,35\n",
                   headers={"Content-Type": "text/csv"})
        assert r.status_code == 422
        assert c.get(f"/days/{DAY}/files").json()["file_count"] == 0


def test_invalid_day_rejected(tmp_path):
    with make_client(tmp_path) as c:
        assert c.post("/days/18-11-2026/upload", content=b"x").status_code == 400
        assert c.get("/days/2026-13-01/records").status_code == 400


# ---------------------------------------------------------------------------
# compaction
# ---------------------------------------------------------------------------

def test_compaction_merges_and_preserves_duplicates(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        upload(c, CSV_DUP)          # two identical rows
        upload(c, CSV_DUP)          # ... uploaded twice

        before = c.get(f"/days/{DAY}/records").json()
        assert before["file_count"] == 4 and before["row_count"] == 8

        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202, r.text
        job = wait_job(c, DAY, r.json()["job_id"])
        assert job["status"] == "done", job
        assert job["input_rows"] == 8 and job["output_rows"] == 8

        after = c.get(f"/days/{DAY}/records").json()
        assert after["file_count"] == 1
        assert after["row_count"] == 8  # duplicates preserved, nothing lost
        key = lambda rec: (rec["buoy_id"], rec["ts_utc"], rec["salinity"])
        assert sorted(map(key, after["records"])) == sorted(map(key, before["records"]))


def test_concurrent_compaction_rejected(tmp_path):
    with make_client(tmp_path, compaction_delay_sec=0.6) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        r1 = c.post(f"/days/{DAY}/compactions")
        assert r1.status_code == 202
        r2 = c.post(f"/days/{DAY}/compactions")
        assert r2.status_code == 409, r2.text
        job = wait_job(c, DAY, r1.json()["job_id"])
        assert job["status"] == "done"
        # after it finishes a new compaction may be started again
        r3 = c.post(f"/days/{DAY}/compactions")
        assert r3.status_code == 202
        assert wait_job(c, DAY, r3.json()["job_id"])["status"] == "done"


def test_upload_and_read_during_compaction(tmp_path):
    """Appends during compaction stay readable and survive the switch."""
    with make_client(tmp_path, compaction_delay_sec=0.8) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202
        job_id = r.json()["job_id"]

        # appended while the compaction is merging its input snapshot
        up = upload(c, CSV_C)
        assert up["rows"] == 1

        # reads never wait for compaction and always see a complete state
        expected = {
            ("B001", "2026-11-18T00:01:00Z", 35.1),
            ("B002", "2026-11-18T00:02:00Z", 35.2),
            ("B001", "2026-11-18T01:01:00Z", 35.3),
            ("B003", "2026-11-18T01:02:00Z", 34.9),
            ("B004", "2026-11-18T02:00:00Z", 36.0),
        }
        for _ in range(10):
            body = c.get(f"/days/{DAY}/records").json()
            got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in body["records"]}
            assert got == expected
            assert body["row_count"] == 5
            assert body["file_count"] in (2, 3)  # pre- or post-switch, never torn
            time.sleep(0.05)

        job = wait_job(c, DAY, job_id)
        assert job["status"] == "done"
        assert job["input_rows"] == 4 and job["output_rows"] == 4

        final = c.get(f"/days/{DAY}/records").json()
        # compacted output + the file appended during compaction
        assert final["file_count"] == 2
        assert final["row_count"] == 5
        got = {(rec["buoy_id"], rec["ts_utc"], rec["salinity"]) for rec in final["records"]}
        assert got == expected


def test_read_is_single_consistent_snapshot(tmp_path):
    """A read must reflect a complete pre- or post-switch registry state."""
    with make_client(tmp_path, compaction_delay_sec=0.5) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]

        lake = c.app.state.lake
        seen = []
        stop = time.time() + 2.0
        while time.time() < stop:
            with lake._state_lock:  # observe exactly what a reader would pin
                inflight = dict(lake._inflight)
            body = c.get(f"/days/{DAY}/records").json()
            seen.append((body["file_count"], body["row_count"]))
            time.sleep(0.02)
        assert wait_job(c, DAY, job_id)["status"] == "done"
        # every observation was a complete state: 2 files/4 rows before the
        # switch, 1 file/4 rows after -- never anything else
        assert set(seen) <= {(2, 4), (1, 4)}
        assert (2, 4) in seen and (1, 4) in seen


def test_compaction_on_empty_day_is_noop(tmp_path):
    with make_client(tmp_path) as c:
        r = c.post(f"/days/{DAY}/compactions")
        assert r.status_code == 202
        job = wait_job(c, DAY, r.json()["job_id"])
        assert job["status"] == "done"
        assert job["output_rows"] == 0
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 0


# ---------------------------------------------------------------------------
# failure handling
# ---------------------------------------------------------------------------

def test_failed_compaction_keeps_original_data(tmp_path, monkeypatch):
    import app.lake as lake_mod

    with make_client(tmp_path) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)

        original = lake_mod.write_parquet_atomic
        def boom(*args, **kwargs):
            raise IOError("disk full (simulated)")
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", boom)

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        job = wait_job(c, DAY, job_id)
        assert job["status"] == "failed"
        assert "disk full" in job["error"]

        # originals untouched and readable
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 2 and body["row_count"] == 4

        # retry succeeds once the "disk" works again
        monkeypatch.setattr(lake_mod, "write_parquet_atomic", original)
        job_id2 = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id2)["status"] == "done"
        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 4


# ---------------------------------------------------------------------------
# garbage collection of retired files
# ---------------------------------------------------------------------------

def test_retired_files_collected_appends_kept(tmp_path):
    with make_client(tmp_path, compaction_delay_sec=0.5, gc_interval_sec=0.1) as c:
        upload(c, CSV_A)
        upload(c, CSV_B)
        data_dir = tmp_path / "data" / f"dt={DAY}"
        old_files = {p.name for p in data_dir.glob("*.parquet")}
        assert len(old_files) == 2

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        upload(c, CSV_C)  # appended during compaction -> must never be collected
        assert wait_job(c, DAY, job_id)["status"] == "done"

        deadline = time.time() + 10
        while time.time() < deadline:
            remaining = {p.name for p in data_dir.glob("*.parquet")}
            if old_files.isdisjoint(remaining):
                break
            time.sleep(0.1)
        else:
            pytest.fail("retired files were not reclaimed")

        remaining = {p.name for p in data_dir.glob("*.parquet")}
        assert len(remaining) == 2  # compacted output + the appended file
        assert any(n.startswith("compacted-") for n in remaining)
        assert any(n.startswith("part-") for n in remaining)
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 5 and body["file_count"] == 2


# ---------------------------------------------------------------------------
# crash recovery
# ---------------------------------------------------------------------------

def test_restart_resumes_interrupted_compaction(tmp_path):
    """A job that was 'running' when the process died is resumed on boot."""
    data_dir = tmp_path / "data"

    lake1 = Lake(str(data_dir))
    lake1.upload(DAY, CSV_A.encode())
    lake1.upload(DAY, CSV_B.encode())
    # simulate a crash mid-compaction: job row persisted as 'running',
    # worker thread never ran (process died)
    with connect(str(lake1.db_path)) as conn:
        rows = conn.execute(
            "SELECT path FROM files WHERE day = ? AND state = 'active'", (DAY,)
        ).fetchall()
        conn.execute(
            "INSERT INTO compactions(day, status, input_paths, input_rows, created_at)"
            " VALUES (?, 'running', ?, ?, ?)",
            (DAY, json.dumps([r["path"] for r in rows]), 4, "2026-11-18T00:00:00Z"),
        )
    del lake1  # "process dies"

    with make_client(tmp_path) as c:
        # recovery resumed the job; wait for it to finish
        deadline = time.time() + 15
        job = None
        while time.time() < deadline:
            jobs = c.get(f"/days/{DAY}/compactions").json()["jobs"]
            assert len(jobs) == 1
            job = jobs[0]
            if job["status"] in ("done", "failed"):
                break
            time.sleep(0.05)
        assert job["status"] == "done", job
        assert job["output_rows"] == 4

        body = c.get(f"/days/{DAY}/records").json()
        assert body["file_count"] == 1 and body["row_count"] == 4


def test_restart_preserves_uploads_and_cleans_orphans(tmp_path):
    data_dir = tmp_path / "data"

    lake1 = Lake(str(data_dir))
    lake1.upload(DAY, CSV_A.encode())
    # orphan: bytes renamed into place but never registered (upload crash),
    # plus a tmp fragment from an interrupted write
    day_dir = data_dir / f"dt={DAY}"
    (day_dir / "part-orphan.parquet").write_bytes(b"not registered")
    (day_dir / "part-x.parquet.tmp-deadbeef").write_bytes(b"partial")
    del lake1

    with make_client(tmp_path) as c:
        body = c.get(f"/days/{DAY}/records").json()
        assert body["row_count"] == 2  # committed upload survived
        remaining = {p.name for p in day_dir.glob("*")}
        assert "part-orphan.parquet" not in remaining
        assert not any(".tmp" in n for n in remaining)


def test_concurrent_reads_during_compaction_are_consistent(tmp_path):
    """Hammer reads from many threads while a compaction switches state."""
    with make_client(tmp_path, compaction_delay_sec=0.4) as c:
        for csv in (CSV_A, CSV_B, CSV_C):
            upload(c, csv)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]

        errors = []
        def reader():
            try:
                while not done.is_set():
                    body = c.get(f"/days/{DAY}/records").json()
                    assert body["row_count"] == 5
                    assert body["file_count"] in (1, 3)
                    assert len(body["records"]) == 5
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        done = threading.Event()
        threads = [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        assert wait_job(c, DAY, job_id)["status"] == "done"
        time.sleep(0.2)
        done.set()
        for t in threads:
            t.join()
        assert not errors, errors


# ---------------------------------------------------------------------------
# daily conflict query + per-row provenance
# ---------------------------------------------------------------------------

CONFLICT_DAY = "2026-11-18"

# B001 at 00:00 has two distinct salinities across two uploads; the third
# CSV repeats 35.1 twice, which must be counted but must not become a
# separate conflict on its own.
CSV_CONF_A = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:00:00Z,35.1\n"
    "B002,2026-11-18T00:05:00Z,34.0\n"
)
CSV_CONF_B = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:00:00Z,35.9\n"
    "B002,2026-11-18T00:05:00Z,34.0\n"
)
CSV_CONF_C = (
    "buoy_id,ts_utc,salinity\n"
    "B001,2026-11-18T00:00:00Z,35.1\n"
    "B001,2026-11-18T00:00:00Z,35.1\n"
)


def conflicts(client: TestClient, day: str = CONFLICT_DAY) -> dict:
    r = client.get(f"/days/{day}/conflicts")
    assert r.status_code == 200, r.text
    return r.json()


def test_conflicts_identify_batches_and_ignore_equal_duplicates(tmp_path):
    with make_client(tmp_path) as c:
        up_a = upload(c, CSV_CONF_A)
        up_b = upload(c, CSV_CONF_B)
        up_c = upload(c, CSV_CONF_C)

        body = conflicts(c)
        assert body["conflict_count"] == 1
        (entry,) = body["conflicts"]
        assert entry["buoy_id"] == "B001"
        assert entry["ts_utc"] == "2026-11-18T00:00:00Z"
        assert entry["salinity_values"] == [35.1, 35.9]

        by_sal = {r["salinity"]: r for r in entry["readings"]}
        assert by_sal[35.1]["count"] == 3          # repeated equal values counted
        assert by_sal[35.9]["count"] == 1
        # equal-valued duplicates did not create additional conflicts
        assert body["conflict_count"] == 1

        src_351 = {s["file"]: s for s in by_sal[35.1]["sources"]}
        assert set(src_351) == {up_a["file"], up_c["file"]}
        assert src_351[up_a["file"]]["row_numbers"] == [1]
        assert src_351[up_a["file"]]["row_count"] == 2
        assert src_351[up_c["file"]]["row_numbers"] == [1, 2]
        assert src_351[up_c["file"]]["row_count"] == 2

        # the non-conflicting buoy at a different instant is absent
        assert all(e["buoy_id"] != "B002" for e in body["conflicts"])


def test_conflicts_survive_compaction_and_reclaim(tmp_path):
    with make_client(tmp_path, gc_interval_sec=0.1) as c:
        up_a = upload(c, CSV_CONF_A)
        up_b = upload(c, CSV_CONF_B)
        upload(c, CSV_CONF_C)
        before = conflicts(c)

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"

        # retirement + unlink of the inputs must not lose provenance
        deadline = time.time() + 10
        day_dir = tmp_path / "data" / f"dt={DAY}"
        while time.time() < deadline:
            if not (day_dir / up_a["file"]).exists() and not (day_dir / up_b["file"]).exists():
                break
            time.sleep(0.05)
        else:
            pytest.fail("retired upload files were not reclaimed")

        after = conflicts(c)
        assert after["conflicts"] == before["conflicts"]  # conflict result unchanged
        assert after["file_count"] == 1
        assert c.get(f"/days/{DAY}/records").json()["file_count"] == 1


def test_records_keep_three_columns_and_duplicates_after_compaction(tmp_path):
    with make_client(tmp_path) as c:
        upload(c, CSV_DUP)
        before = c.get(f"/days/{DAY}/records").json()
        assert before["row_count"] == 2
        assert all(set(rec) == {"buoy_id", "ts_utc", "salinity"} for rec in before["records"])

        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"

        after = c.get(f"/days/{DAY}/records").json()
        assert after["row_count"] == 2             # duplicate rows preserved
        assert all(set(rec) == {"buoy_id", "ts_utc", "salinity"} for rec in after["records"])
        assert sorted((r["buoy_id"], r["ts_utc"], r["salinity"]) for r in after["records"]) == \
               sorted((r["buoy_id"], r["ts_utc"], r["salinity"]) for r in before["records"])


def test_appends_during_compaction_appear_in_later_conflict_query(tmp_path):
    """Rows appended while a compaction runs show up afterwards, still sourced."""
    with make_client(tmp_path, compaction_delay_sec=0.5) as c:
        upload(c, CSV_CONF_A)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        during = upload(c, CSV_CONF_B)            # appended mid-compaction

        # every query sees a complete pre- or post-switch file set: the new
        # row is present in both, so the conflict is always reported
        for _ in range(8):
            body = conflicts(c)
            assert body["conflict_count"] == 1
            assert during["file"] in {
                s["file"] for s in body["conflicts"][0]["readings"][1]["sources"]
            }
            time.sleep(0.03)

        assert wait_job(c, DAY, job_id)["status"] == "done"
        body = conflicts(c)
        assert body["conflict_count"] == 1
        assert c.get(f"/days/{DAY}/records").json()["file_count"] == 2


def test_legacy_uncompacted_file_uses_own_filename(tmp_path):
    """A pre-provenance upload file is attributed to its own filename."""
    data_dir = tmp_path / "data"
    lake = Lake(str(data_dir))
    lake.upload(DAY, CSV_CONF_A.encode())
    lake.upload(DAY, CSV_CONF_B.encode())

    # rewrite one file in the legacy 3-column format, keeping its registration
    with connect(str(lake.db_path)) as conn:
        path = conn.execute(
            "SELECT path FROM files WHERE state = 'active' ORDER BY id LIMIT 1"
        ).fetchone()["path"]
    legacy = pa.table(
        {
            "buoy_id": ["B001"],
            "ts_utc": [datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc)],
            "salinity": [35.1],
        },
        schema=CANONICAL_SCHEMA,
    )
    pq.write_table(legacy, path)
    del lake

    with make_client(tmp_path) as c:
        body = conflicts(c)
        assert body["conflict_count"] == 1
        readings = {r["salinity"]: r for r in body["conflicts"][0]["readings"]}
        assert readings[35.1]["file"] == Path(path).name    # its own filename
        assert readings[35.1]["source_known"] is True


def test_legacy_compacted_rows_have_unknown_source(tmp_path):
    """A pre-provenance compaction output must not claim its own filename."""
    data_dir = tmp_path / "data"
    lake = Lake(str(data_dir))
    legacy = pa.table(
        {
            "buoy_id": ["B001", "B001"],
            "ts_utc": [
                datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc),
            ],
            "salinity": [35.1, 35.9],
        },
        schema=CANONICAL_SCHEMA,
    )
    day_dir = data_dir / f"dt={DAY}"
    day_dir.mkdir(parents=True)
    legacy_path = day_dir / "compacted-1-legacy.parquet"
    pq.write_table(legacy, legacy_path)
    with connect(str(lake.db_path)) as conn:
        conn.execute(
            "INSERT INTO files(day, path, kind, state, rows, created_at)"
            " VALUES (?, ?, 'compacted', 'active', 2, '2026-11-18T00:00:00Z')",
            (DAY, str(legacy_path)),
        )
    del lake

    with make_client(tmp_path) as c:
        body = conflicts(c)
        assert body["conflict_count"] == 1
        for reading in body["conflicts"][0]["readings"]:
            assert reading["file"] is None
            assert reading["source_known"] is False
            assert reading["sources"][0]["file"] is None
            assert legacy_path.name not in json.dumps(body)
        # the original three-column read still works
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 2


def test_mixed_legacy_and_current_files_read_and_query(tmp_path):
    """Mixing a legacy 3-column file with new files must work for both APIs."""
    data_dir = tmp_path / "data"
    lake = Lake(str(data_dir))
    lake.upload(DAY, CSV_CONF_B.encode())  # current format: 35.9 at 00:00

    legacy = pa.table(
        {
            "buoy_id": ["B001"],
            "ts_utc": [datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc)],
            "salinity": [35.1],
        },
        schema=CANONICAL_SCHEMA,
    )
    day_dir = data_dir / f"dt={DAY}"
    legacy_path = day_dir / "part-legacy.parquet"
    pq.write_table(legacy, legacy_path)
    with connect(str(lake.db_path)) as conn:
        conn.execute(
            "INSERT INTO files(day, path, kind, state, rows, created_at)"
            " VALUES (?, ?, 'upload', 'active', 1, '2026-11-18T00:00:00Z')",
            (DAY, str(legacy_path)),
        )
    del lake

    with make_client(tmp_path) as c:
        recs = c.get(f"/days/{DAY}/records").json()
        assert recs["row_count"] == 3
        assert recs["file_count"] == 2
        assert all(set(r) == {"buoy_id", "ts_utc", "salinity"} for r in recs["records"])

        body = conflicts(c)
        assert body["conflict_count"] == 1
        readings = {r["salinity"]: r for r in body["conflicts"][0]["readings"]}
        assert readings[35.1]["file"] == "part-legacy.parquet"
        assert readings[35.9]["file"] != "part-legacy.parquet"

        # compaction over the mixed set keeps both sources distinguishable
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"
        after = conflicts(c)
        readings = {r["salinity"]: r for r in after["conflicts"][0]["readings"]}
        assert readings[35.1]["file"] == "part-legacy.parquet"
        assert readings[35.9]["file"] != "part-legacy.parquet"


def test_failed_compaction_does_not_change_conflicts(tmp_path, monkeypatch):
    import app.lake as lake_mod

    with make_client(tmp_path) as c:
        upload(c, CSV_CONF_A)
        upload(c, CSV_CONF_B)
        upload(c, CSV_CONF_C)
        before = conflicts(c)

        monkeypatch.setattr(
            lake_mod, "write_parquet_atomic",
            lambda *a, **k: (_ for _ in ()).throw(IOError("disk full (simulated)")),
        )
        job = wait_job(c, DAY, c.post(f"/days/{DAY}/compactions").json()["job_id"])
        assert job["status"] == "failed"

        assert conflicts(c) == before
        assert c.get(f"/days/{DAY}/records").json()["row_count"] == 6


def test_conflicts_ignore_non_finite_and_incomplete(tmp_path):
    """Only finite salinities participate; NaN never creates or masks conflicts."""
    with make_client(tmp_path) as c:
        upload(c, "buoy_id,ts_utc,salinity\n"
                 "B001,2026-11-18T00:00:00Z,nan\n"
                 "B001,2026-11-18T00:00:00Z,35.1\n"
                 "B001,2026-11-18T00:00:00Z,35.1\n")
        body = conflicts(c)
        assert body["conflict_count"] == 0
        assert body["readings"] == []


def test_conflicts_query_is_single_consistent_snapshot(tmp_path):
    """A conflict query uses the complete pre- or post-switch file set."""
    with make_client(tmp_path, compaction_delay_sec=0.4) as c:
        upload(c, CSV_CONF_A)
        upload(c, CSV_CONF_B)
        upload(c, CSV_CONF_C)
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]

        seen = []
        errors = []
        deadline = time.time() + 2.0
        while time.time() < deadline:
            try:
                body = conflicts(c)
                # 1 conflicting (buoy, instant), 2 distinct salinities, 4 rows
                assert body["conflict_count"] == 1
                assert sum(r["count"] for r in body["conflicts"][0]["readings"]) == 4
                seen.append(body["file_count"])
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
            time.sleep(0.01)
        assert wait_job(c, DAY, job_id)["status"] == "done"
        assert not errors, errors
        assert set(seen) <= {1, 3}
        assert 3 in seen and 1 in seen

        after = conflicts(c)
        assert after["conflict_count"] == 1
        assert after["file_count"] == 1


def test_conflicts_endpoint_validates_day(tmp_path):
    with make_client(tmp_path) as c:
        assert c.get("/days/2026-13-01/conflicts").status_code == 400
        assert c.get("/days/18-11-2026/conflicts").status_code == 400


def test_conflicts_empty_day(tmp_path):
    with make_client(tmp_path) as c:
        body = conflicts(c)
        assert body["conflict_count"] == 0
        assert body["conflicts"] == [] and body["readings"] == []


def _write_legacy(day_dir: Path, name: str, rows: list[tuple[str, datetime, float]]) -> Path:
    """Write a pre-provenance (3-column) parquet file."""
    path = day_dir / name
    table = pa.table(
        {
            "buoy_id": [r[0] for r in rows],
            "ts_utc": [r[1] for r in rows],
            "salinity": [r[2] for r in rows],
        },
        schema=CANONICAL_SCHEMA,
    )
    pq.write_table(table, path)
    return path


def test_legacy_compacted_conflict_with_new_upload_stays_unknown(tmp_path):
    """An untraceable legacy compaction output conflicts with a new upload.

    The legacy rows must report unknown provenance; only the new upload's
    rows name a batch, and the compaction output's own name is never claimed.
    """
    data_dir = tmp_path / "data"
    lake = Lake(str(data_dir))
    day_dir = data_dir / f"dt={DAY}"
    day_dir.mkdir(parents=True, exist_ok=True)
    legacy_path = _write_legacy(
        day_dir,
        "compacted-7-deadbeef.parquet",
        [("B001", datetime(2026, 11, 18, 0, 0, tzinfo=timezone.utc), 35.1)],
    )
    with connect(str(lake.db_path)) as conn:
        conn.execute(
            "INSERT INTO files(day, path, kind, state, rows, created_at)"
            " VALUES (?, ?, 'compacted', 'active', 1, '2026-11-18T00:00:00Z')",
            (DAY, str(legacy_path)),
        )
    del lake

    with make_client(tmp_path) as c:
        up = upload(c, "buoy_id,ts_utc,salinity\nB001,2026-11-18T00:00:00Z,35.9\n")
        body = conflicts(c)
        assert body["conflict_count"] == 1
        readings = {r["salinity"]: r for r in body["conflicts"][0]["readings"]}

        assert readings[35.1]["source_known"] is False
        assert readings[35.1]["source_file"] is None
        assert readings[35.1]["row_count"] is None
        assert readings[35.1]["salinity"] == 35.1

        assert readings[35.9]["source_known"] is True
        assert readings[35.9]["source_file"] == up["file"]
        assert readings[35.9]["row_numbers"] == [1]

        # the output filename appears nowhere as a claimed source
        assert legacy_path.name not in json.dumps(body)

        # compacting the mixed set keeps the unknown rows unknown
        job_id = c.post(f"/days/{DAY}/compactions").json()["job_id"]
        assert wait_job(c, DAY, job_id)["status"] == "done"
        after = conflicts(c)
        assert after["conflict_count"] == 1
        readings = {r["salinity"]: r for r in after["conflicts"][0]["readings"]}
        assert readings[35.1]["source_known"] is False
        assert readings[35.9]["source_file"] == up["file"]
        assert legacy_path.name not in json.dumps(after)


def test_conflicts_across_three_uploads_and_multiple_groups(tmp_path):
    with make_client(tmp_path) as c:
        up1 = upload(c, "buoy_id,ts_utc,salinity\n"
                        "B1,2026-11-18T00:00:00Z,35.0\n"
                        "B2,2026-11-18T00:00:00Z,34.0\n")
        up2 = upload(c, "buoy_id,ts_utc,salinity\n"
                        "B1,2026-11-18T00:00:00Z,35.5\n"
                        "B3,2026-11-18T00:10:00Z,33.0\n")
        up3 = upload(c, "buoy_id,ts_utc,salinity\n"
                        "B3,2026-11-18T00:10:00Z,33.8\n"
                        "B3,2026-11-18T00:10:00Z,33.8\n")

        body = conflicts(c)
        assert body["conflict_count"] == 2
        keyed = {(e["buoy_id"], e["ts_utc"]): e for e in body["conflicts"]}

        b1 = keyed[("B1", "2026-11-18T00:00:00Z")]
        assert {r["source_file"] for r in b1["readings"]} == {up1["file"], up2["file"]}

        b3 = keyed[("B3", "2026-11-18T00:10:00Z")]
        r33 = next(r for r in b3["readings"] if r["salinity"] == 33.8)
        assert r33["count"] == 2                       # same-value duplicate counted
        assert r33["source_file"] == up3["file"]
        assert r33["row_numbers"] == [1, 2]
        assert r33["row_count"] == 2
        assert len(b3["readings"]) == 2                # only two distinct values

        assert len(body["readings"]) == 4              # 2 readings per conflict
