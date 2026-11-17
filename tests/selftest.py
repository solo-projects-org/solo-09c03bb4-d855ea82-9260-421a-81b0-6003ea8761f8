#!/usr/bin/env python3
"""End-to-end self test for the ensemble rainfall probability API.

By default the script spawns TWO uvicorn processes sharing one SQLite file
(dual-instance deployment), waits for health, and exercises every key flow:

  * registration / member-count / threshold / datetime validation
  * idempotent resubmit (same revision+content) and conflicting resubmit rejection
  * revision ordering, grid-shape and non-negativity checks
  * publish blocked until every member is present
  * probability / denominator / all-missing null semantics and used revisions
  * correction immediately expires the old product while history stays queryable
  * two-instance interleaved publish vs. correction -> only consistent products
  * parallel duplicate publishes collapse to one idempotent product
  * injected compute failure leaves no half-written product (atomicity)
  * slow computation never blocks another forecast time's ingestion
  * Swagger UI / OpenAPI availability

Only the Python standard library is required.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PASS_COUNT = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS_COUNT
    if not cond:
        raise AssertionError(f"FAIL: {name} {detail}")
    PASS_COUNT += 1
    print(f"  PASS  {name}")


def http(method: str, base: str, path: str, body: Optional[dict] = None) -> Tuple[int, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def approx(a: Any, b: Any, tol: float = 1e-9) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def matrix_approx(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for x, y in zip(ra, rb):
            if not approx(x, y):
                return False
    return True


# --------------------------------------------------------------------------- #
# dual-instance process management
# --------------------------------------------------------------------------- #
def spawn_servers() -> Tuple[list, str, str, str, str]:
    tmp = tempfile.mkdtemp(prefix="rain-selftest-")
    db_path = os.path.join(tmp, "rain.db")
    fail_marker = os.path.join(tmp, "fail.marker")
    env = dict(os.environ)
    env.update(
        RAIN_DB_PATH=db_path,
        RAIN_TEST_PUBLISH_DELAY_S="0.6",
        RAIN_TEST_ANALYSIS_DELAY_S="0.6",
        RAIN_TEST_FAIL_MARKER=fail_marker,
        PYTHONPATH=ROOT,
    )
    procs = []
    for port in (8091, 8092):
        log = open(os.path.join(tmp, f"uvicorn-{port}.log"), "w")
        procs.append(
            subprocess.Popen(
                [sys.executable, "-m", "uvicorn", "app.main:app",
                 "--host", "127.0.0.1", "--port", str(port)],
                cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
            )
        )
    base_a, base_b = "http://127.0.0.1:8091", "http://127.0.0.1:8092"
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            s1, _ = http("GET", base_a, "/health")
            s2, _ = http("GET", base_b, "/health")
            if s1 == 200 and s2 == 200:
                return procs, base_a, base_b, fail_marker, tmp
        except Exception:
            pass
        time.sleep(0.2)
    shutdown(procs)
    raise RuntimeError(f"servers did not become healthy (logs in {tmp})")


def shutdown(procs: list) -> None:
    for p in procs:
        p.send_signal(signal.SIGINT)
    for p in procs:
        try:
            p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill()


def main() -> int:
    base_a = os.environ.get("RAIN_BASE_A")
    base_b = os.environ.get("RAIN_BASE_B")
    fail_marker = os.environ.get("RAIN_TEST_FAIL_MARKER", "")
    spawn_procs: list = []
    if not base_a or not base_b:
        spawn_procs, base_a, base_b, fail_marker, _ = spawn_servers()
        print(f"[setup] spawned dual instances {base_a} / {base_b}")
    hooks_enabled = bool(fail_marker)
    try:
        run(base_a, base_b, fail_marker, hooks_enabled)
    finally:
        if spawn_procs:
            shutdown(spawn_procs)
    print(f"\nALL TESTS PASSED ({PASS_COUNT} assertions)")
    return 0


def register(base: str, members: int = 3, rows: int = 2, cols: int = 2,
             thresholds=(5.0, 10.0), names=None,
             forecast_time: str = "2026-11-16T00:00:00Z") -> dict:
    if names is None:
        names = [f"m{i}" for i in range(1, members + 1)]
    body = {
        "forecast_time": forecast_time,
        "rows": rows, "cols": cols,
        "member_names": names,
        "thresholds": list(thresholds),
    }
    status, data = http("POST", base, "/groups", body)
    assert status == 201, (status, data)
    return data


def analysis(base: str, group_ids, threshold=5.0, line=0.5) -> Tuple[int, Any]:
    return http("POST", base, "/analysis/persistent-high-risk", {
        "group_ids": list(group_ids),
        "threshold": threshold,
        "probability_threshold": line,
    })


def run(base_a: str, base_b: str, fail_marker: str, hooks_enabled: bool) -> None:
    print("\n[1] validation on registration")
    bad = {"forecast_time": "2026-11-16T00:00:00Z", "rows": 2, "cols": 2,
           "member_names": ["only-one"], "thresholds": [5.0]}
    s, d = http("POST", base_a, "/groups", bad)
    check("1 member rejected (need 2-8)", s == 422, str(d))
    bad["member_names"] = [f"m{i}" for i in range(9)]
    s, d = http("POST", base_a, "/groups", bad)
    check("9 members rejected", s == 422)
    bad["member_names"] = ["a", "b"]
    bad["thresholds"] = [-1.0]
    s, d = http("POST", base_a, "/groups", bad)
    check("negative threshold rejected", s == 422)
    bad["thresholds"] = [5.0]
    bad["forecast_time"] = "2026-11-16T00:00:00"  # naive, no timezone
    s, d = http("POST", base_a, "/groups", bad)
    check("naive datetime rejected", s == 422)

    print("\n[2] happy-path group + member submission semantics")
    g = register(base_a, members=3, thresholds=[5.0, 10.0])
    gid = g["group_id"]
    check("registered 3 members", len(g["members"]) == 3)
    s, d = http("GET", base_b, f"/groups/{gid}")
    check("group visible from instance B", s == 200 and d["all_members_present"] is False)

    rain1 = [[0.0, 10.0], [20.0, 0.0]]
    s, d = http("PUT", base_a, f"/groups/{gid}/members/m1",
                {"rain": rain1, "revision": 1})
    check("m1 rev1 accepted", s == 200 and d["duplicate"] is False, str(d))
    s, d2 = http("PUT", base_b, f"/groups/{gid}/members/m1",
                 {"rain": rain1, "revision": 1})
    check("same revision+content resent is idempotent", s == 200 and d2["duplicate"] is True)
    check("idempotent hash stable", d["content_hash"] == d2["content_hash"])
    s, d = http("PUT", base_a, f"/groups/{gid}/members/m1",
                {"rain": [[0.0, 10.0], [20.0, 1.0]], "revision": 1})
    check("same revision different content rejected (异文拒收)",
          s == 409 and d["error"]["code"] == "REVISION_CONTENT_CONFLICT", str(d))
    s, d = http("PUT", base_a, f"/groups/{gid}/members/ghost",
                {"rain": rain1, "revision": 1})
    check("unknown member -> 404", s == 404 and d["error"]["code"] == "MEMBER_NOT_FOUND")
    s, d = http("PUT", base_a, "/groups/nope/members/m1", {"rain": rain1, "revision": 1})
    check("unknown group -> 404", s == 404 and d["error"]["code"] == "GROUP_NOT_FOUND")

    print("\n[3] revision ordering and grid validation on a second group")
    g2 = register(base_b, members=2, thresholds=[5.0])
    gid2 = g2["group_id"]
    s, d = http("PUT", base_a, f"/groups/{gid2}/members/m1",
                {"rain": [[1.0, 2.0], [3.0, 4.0]], "revision": 1})
    check("g2 m1 rev1", s == 200)
    s, d = http("PUT", base_b, f"/groups/{gid2}/members/m1",
                {"rain": [[9.0, 9.0], [9.0, 9.0]], "revision": 2})
    check("g2 m1 rev2", s == 200)
    s, d = http("PUT", base_a, f"/groups/{gid2}/members/m1",
                {"rain": [[1.0, 2.0], [3.0, 4.0]], "revision": 1})
    check("lower revision rejected", s == 409 and d["error"]["code"] == "REVISION_CONFLICT")
    s, d = http("PUT", base_a, f"/groups/{gid2}/members/m1",
                {"rain": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], "revision": 3})
    check("grid shape mismatch rejected", s == 422 and d["error"]["code"] == "GRID_MISMATCH")
    s, d = http("PUT", base_a, f"/groups/{gid2}/members/m1",
                {"rain": [[-1.0, 2.0], [3.0, 4.0]], "revision": 3})
    check("negative rain rejected", s == 422 and d["error"]["code"] == "NEGATIVE_RAIN")
    s, d = http("PUT", base_a, f"/groups/{gid2}/members/m1",
                {"rain": [[1.0, 2.0], [3.0, 4.0]], "missing": [[1, 0, 0]], "revision": 3})
    check("missing grid shape mismatch rejected",
          s == 422 and d["error"]["code"] == "GRID_MISMATCH", str(d))

    print("\n[4] publishing requires every member")
    s, d = http("POST", base_a, f"/groups/{gid}/publish")
    check("incomplete group cannot publish",
          s == 409 and d["error"]["code"] == "MEMBERS_INCOMPLETE", str(d))

    print("\n[5] complete group and verify probability math")
    rain2 = [[10.0, 10.0], [0.0, 0.0]]
    rain3 = [[5.0, 0.0], [99.0, 0.0]]
    miss3 = [[0, 0], [1, 1]]  # m3 missing on second row
    s, _ = http("PUT", base_b, f"/groups/{gid}/members/m2",
                {"rain": rain2, "revision": 1})
    check("m2 rev1 via instance B", s == 200)
    s, _ = http("PUT", base_a, f"/groups/{gid}/members/m3",
                {"rain": rain3, "missing": miss3, "revision": 1})
    check("m3 rev1 with missing grid", s == 200)

    s, p1 = http("POST", base_a, f"/groups/{gid}/publish")
    check("publish succeeds when complete", s == 200, str(p1))
    check("product marked current", p1["is_current"] is True)
    check("3 member revisions recorded",
          {m["member_id"]: m["revision"] for m in p1["members_used"]}
          == {"m1": 1, "m2": 1, "m3": 1})
    by_thr = {t["threshold"]: t for t in p1["thresholds"]}
    check("denominators exclude missing members",
          by_thr[5.0]["denominator"] == [[3, 3], [2, 2]],
          str(by_thr[5.0]["denominator"]))
    check("probability threshold=5",
          matrix_approx(by_thr[5.0]["probability"],
                        [[2 / 3, 2 / 3], [0.5, 0.0]]),
          str(by_thr[5.0]["probability"]))
    check("probability threshold=10",
          matrix_approx(by_thr[10.0]["probability"],
                        [[1 / 3, 2 / 3], [0.5, 0.0]]),
          str(by_thr[10.0]["probability"]))
    s, p1b = http("POST", base_b, f"/groups/{gid}/publish")
    check("republish identical revision set is idempotent",
          s == 200 and p1b["product_id"] == p1["product_id"])
    s, cur = http("GET", base_a, f"/groups/{gid}/products/current")
    check("current product query", s == 200 and cur["product_id"] == p1["product_id"])
    s, listing = http("GET", base_b, f"/groups/{gid}/products")
    check("one product in history listing", s == 200 and len(listing["products"]) == 1)
    s, d = http("GET", base_a, f"/groups/{gid2}/products/{p1['product_id']}")
    check("product from other group -> 404", s == 404)

    print("\n[6] all-missing cell yields null probability and zero denominator")
    g3 = register(base_a, members=2, rows=1, cols=2, thresholds=[5.0])
    gid3 = g3["group_id"]
    http("PUT", base_a, f"/groups/{gid3}/members/m1",
         {"rain": [[5.0, 9.0]], "missing": [[0, 1]], "revision": 1})
    http("PUT", base_b, f"/groups/{gid3}/members/m2",
         {"rain": [[10.0, 9.0]], "missing": [[0, 1]], "revision": 1})
    s, p3 = http("POST", base_a, f"/groups/{gid3}/publish")
    t3 = p3["thresholds"][0]
    check("all-missing cell null", t3["probability"] == [[1.0, None]], str(t3))
    check("all-missing cell denominator 0", t3["denominator"] == [[2, 0]])

    print("\n[7] correction expires old product immediately, history retained")
    s, _ = http("PUT", base_b, f"/groups/{gid}/members/m1",
                {"rain": [[0.0, 10.0], [20.0, 5.0]], "revision": 2})
    check("correction rev2 accepted", s == 200)
    s, d = http("GET", base_a, f"/groups/{gid}/products/current")
    check("no stale product served as current warning basis",
          s == 404 and d["error"]["code"] == "NO_CURRENT_PRODUCT", str(d))
    s, listing = http("GET", base_b, f"/groups/{gid}/products")
    check("old product retained but superseded",
          len(listing["products"]) == 1 and listing["products"][0]["is_current"] is False)
    s, hist = http("GET", base_a, f"/groups/{gid}/products/{p1['product_id']}")
    check("historical product still queryable", s == 200 and hist["is_current"] is False)
    s, p2 = http("POST", base_b, f"/groups/{gid}/publish")
    check("republish after correction", s == 200 and p2["product_id"] != p1["product_id"])
    check("new product uses rev2 for corrected member",
          {m["member_id"]: m["revision"] for m in p2["members_used"]}
          == {"m1": 2, "m2": 1, "m3": 1})
    s, cur = http("GET", base_b, f"/groups/{gid}/products/current")
    check("current points at new product", cur["product_id"] == p2["product_id"])

    print("\n[8] dual-instance interleaving: publish vs concurrent correction")
    g4 = register(base_a, members=3, rows=64, cols=64, thresholds=[5.0, 20.0])
    gid4 = g4["group_id"]
    for i, inst in enumerate((base_a, base_b, base_a)):
        name = f"m{i + 1}"
        grid = [[float((i + 1) * (r + c) % 25) for c in range(64)] for r in range(64)]
        s, _ = http("PUT", inst, f"/groups/{gid4}/members/{name}",
                    {"rain": grid, "revision": 1})
        assert s == 200

    result: Dict[str, Any] = {}

    def worker_publish() -> None:
        result["status"], result["body"] = http("POST", base_a, f"/groups/{gid4}/publish")

    t = threading.Thread(target=worker_publish)
    t.start()
    time.sleep(0.15)  # let the publish enter its slow compute phase
    t0 = time.time()
    s, corr = http("PUT", base_b, f"/groups/{gid4}/members/m2",
                   {"rain": [[7.0] * 64 for _ in range(64)], "revision": 2})
    write_elapsed = time.time() - t0
    check("correction write not blocked by in-flight compute", s == 200, str(corr))
    if hooks_enabled:
        check("ingest stayed fast while compute was running", write_elapsed < 0.5,
              f"{write_elapsed:.2f}s")
    t.join()
    check("in-flight publish eventually returns 200", result["status"] == 200, str(result))

    s, status4 = http("GET", base_a, f"/groups/{gid4}")
    latest = {m["member_id"]: m["latest_revision"] for m in status4["members"]}
    s, cur4 = http("GET", base_b, f"/groups/{gid4}/products/current")
    check("a current product exists after interleaving", s == 200)
    cur_map = {m["member_id"]: m["revision"] for m in cur4["members_used"]}
    check("current product matches latest member revisions (no mixed revision group)",
          cur_map == latest, f"{cur_map} vs {latest}")
    s, listing4 = http("GET", base_a, f"/groups/{gid4}/products")
    for prod in listing4["products"]:
        check(f"product {prod['product_id']} has complete revision set",
              len(prod["revision_map"]) == 3)
        if prod["is_current"]:
            check("only one current product",
                  sum(1 for x in listing4["products"] if x["is_current"]) == 1)
    check("current listing entry consistent",
          [x for x in listing4["products"] if x["is_current"]][0]["product_id"]
          == cur4["product_id"])

    print("\n[9] parallel publishes of the same revision set collapse to one product")
    g5 = register(base_b, members=2, rows=8, cols=8, thresholds=[5.0])
    gid5 = g5["group_id"]
    http("PUT", base_a, f"/groups/{gid5}/members/m1",
         {"rain": [[6.0] * 8 for _ in range(8)], "revision": 1})
    http("PUT", base_b, f"/groups/{gid5}/members/m2",
         {"rain": [[0.0] * 8 for _ in range(8)], "revision": 1})
    outs: Dict[str, Any] = {}

    def pub(instance: str) -> None:
        outs[instance] = http("POST", instance, f"/groups/{gid5}/publish")

    threads = [threading.Thread(target=pub, args=(base_a,)),
               threading.Thread(target=pub, args=(base_b,))]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    ids = {body["product_id"] for st, body in outs.values() if st == 200}
    check("both parallel publishes succeed", all(st == 200 for st, _ in outs.values()))
    check("both return the same idempotent product", len(ids) == 1, str(ids))
    s, listing5 = http("GET", base_a, f"/groups/{gid5}/products")
    check("exactly one product row for the revision set", len(listing5["products"]) == 1)

    print("\n[10] compute failure leaves no half-written product")
    g6 = register(base_a, members=2, rows=4, cols=4, thresholds=[5.0])
    gid6 = g6["group_id"]
    http("PUT", base_a, f"/groups/{gid6}/members/m1",
         {"rain": [[6.0] * 4 for _ in range(4)], "revision": 1})
    http("PUT", base_b, f"/groups/{gid6}/members/m2",
         {"rain": [[4.0] * 4 for _ in range(4)], "revision": 1})
    if hooks_enabled:
        with open(fail_marker, "w") as fh:
            fh.write("fail")
        s, d = http("POST", base_a, f"/groups/{gid6}/publish")
        os.remove(fail_marker)
        check("injected failure surfaces as 500", s == 500 and d["error"]["code"] == "COMPUTE_FAILED")
        s, d = http("GET", base_b, f"/groups/{gid6}/products/current")
        check("no current product after failed compute", s == 404)
        s, listing6 = http("GET", base_a, f"/groups/{gid6}/products")
        check("no product rows at all after failed compute", listing6["products"] == [])
        s, p6 = http("POST", base_b, f"/groups/{gid6}/publish")
        check("publish succeeds after failure cleared", s == 200 and p6["is_current"])
    else:
        print("  SKIP  failure injection (RAIN_TEST_FAIL_MARKER not configured)")

    print("\n[11] slow compute on one time does not block another time's ingestion")
    g7 = register(base_a, members=2, rows=16, cols=16, thresholds=[5.0])
    gid7 = g7["group_id"]
    http("PUT", base_a, f"/groups/{gid7}/members/m1",
         {"rain": [[6.0] * 16 for _ in range(16)], "revision": 1})
    http("PUT", base_a, f"/groups/{gid7}/members/m2",
         {"rain": [[6.0] * 16 for _ in range(16)], "revision": 1})

    bg = {}

    def slow_publish() -> None:
        bg["result"] = http("POST", base_a, f"/groups/{gid7}/publish")

    th = threading.Thread(target=slow_publish)
    th.start()
    time.sleep(0.15)
    t0 = time.time()
    g8 = register(base_b, members=2, rows=3, cols=3, thresholds=[5.0])
    http("PUT", base_b, f"/groups/{g8['group_id']}/members/m1",
         {"rain": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], "revision": 1})
    s, _ = http("PUT", base_b, f"/groups/{g8['group_id']}/members/m2",
                {"rain": [[9.0, 8.0, 7.0], [6.0, 5.0, 4.0], [3.0, 2.0, 1.0]], "revision": 1})
    ingest_elapsed = time.time() - t0
    check("other time registration+ingest succeeded during slow compute", s == 200)
    if hooks_enabled:
        check("other time ingest stayed fast", ingest_elapsed < 0.5, f"{ingest_elapsed:.2f}s")
    th.join()
    check("slow publish completed", bg["result"][0] == 200, str(bg))

    print("\n[12] Swagger / OpenAPI")
    s, openapi = http("GET", base_a, "/openapi.json")
    check("openapi.json served", s == 200 and "paths" in openapi)
    for path in ("/groups", "/groups/{group_id}/members/{member_id}",
                 "/groups/{group_id}/publish",
                 "/groups/{group_id}/products/current",
                 "/analysis/persistent-high-risk"):
        check(f"path {path} documented", path in openapi["paths"])
    req = urllib.request.Request(base_a + "/docs")
    with urllib.request.urlopen(req, timeout=10) as resp:
        check("Swagger UI served", resp.status == 200 and b"swagger" in resp.read().lower())

    run_analysis_tests(base_a, base_b, fail_marker, hooks_enabled)


def _grid(values: list, rows: int, cols: int) -> list:
    """Reshape a flat list into a rows x cols matrix (already-nested input passes through)."""
    if values and isinstance(values[0], list):
        return values
    flat = list(values)
    assert len(flat) == rows * cols, (len(flat), rows, cols)
    return [flat[r * cols:(r + 1) * cols] for r in range(rows)]


def _build_series(base: str, specs: Dict[str, Dict[str, Any]], rows: int = 1, cols: int = 5,
                  thresholds=(5.0, 10.0)):
    """Register + publish one 2-member time step per spec; returns {name: (gid, pid)}."""
    out: Dict[str, Tuple[str, int]] = {}
    for name, spec in specs.items():
        g = register(base, members=2, rows=rows, cols=cols, thresholds=thresholds,
                     forecast_time=spec["time"])
        gid = g["group_id"]
        for member in ("m1", "m2"):
            body: Dict[str, Any] = {
                "rain": _grid(spec[member], rows, cols),
                "revision": 1,
            }
            if spec.get("miss") is not None:
                body["missing"] = _grid(spec["miss"], rows, cols)
            s, d = http("PUT", base, f"/groups/{gid}/members/{member}", body)
            assert s == 200, (s, d)
        s, p = http("POST", base, f"/groups/{gid}/publish")
        assert s == 200, (s, p)
        out[name] = (gid, p["product_id"])
    return out


def run_analysis_tests(base_a: str, base_b: str, fail_marker: str, hooks_enabled: bool) -> None:
    print("\n[13] cross-time persistent high-risk analysis: streak semantics")
    # 2 members -> per-cell probability in {0, 0.5, 1}; threshold 5, line 0.5.
    # c0 1,1,1,1     -> first confirmed at step 2
    # c1 1,0,1,1     -> first confirmed at step 4
    # c2 1,null,1,1  -> the null breaks the t0/t1 streak, so first confirmed at
    #                   step 4 (it would be step 2 if a gap did not interrupt)
    # c3 .5,.5,.5,.5 -> boundary equality qualifies; first confirmed at step 2
    # c4 0,0,0,0     -> never
    specs = {
        "t0": {"time": "2026-11-16T00:00:00Z",
               "m1": [10, 10, 10, 10, 0], "m2": [10, 10, 10, 0, 0]},
        "t1": {"time": "2026-11-16T01:00:00Z", "miss": [0, 0, 1, 0, 0],
               "m1": [10, 0, 99, 10, 0], "m2": [10, 0, 99, 0, 0]},
        "t2": {"time": "2026-11-16T02:00:00Z",
               "m1": [10, 10, 10, 10, 0], "m2": [10, 10, 10, 0, 0]},
        "t3": {"time": "2026-11-16T03:00:00Z",
               "m1": [10, 10, 10, 10, 0], "m2": [10, 10, 10, 0, 0]},
    }
    series = _build_series(base_a, specs)
    t0, t1, t2, t3 = (series[k] for k in ("t0", "t1", "t2", "t3"))
    order = [t0[0], t1[0], t2[0], t3[0]]

    s, a = analysis(base_b, order, threshold=5.0, line=0.5)
    check("analysis succeeds on four published times", s == 200, str(a))
    check("all four product ids listed in order",
          a["product_ids"] == [t0[1], t1[1], t2[1], t3[1]], str(a["product_ids"]))
    check("products_used carries id/time/revisions",
          [p["product_id"] for p in a["products_used"]] == a["product_ids"]
          and [p["forecast_time"] for p in a["products_used"]] == a["forecast_times"])
    check("first confirmed steps are second round of each streak",
          a["first_confirmed_step"] == [[2, 4, 4, 2, None]], str(a["first_confirmed_step"]))
    check("null-probability round breaks the streak",
          a["first_confirmed_step"][0][2] == 4,
          f"cell c2 must wait for the post-null streak, got {a['first_confirmed_step'][0][2]}")
    check("first confirmed forecast times",
          a["first_confirmed_forecast_time"] ==
          [["2026-11-16T01:00:00+00:00", "2026-11-16T03:00:00+00:00",
            "2026-11-16T03:00:00+00:00", "2026-11-16T01:00:00+00:00", None]],
          str(a["first_confirmed_forecast_time"]))
    check("first confirmed group ids",
          a["first_confirmed_group_id"] == [[t1[0], t3[0], t3[0], t1[0], None]],
          str(a["first_confirmed_group_id"]))
    check("first confirmed product ids",
          a["first_confirmed_product_id"] == [[t1[1], t3[1], t3[1], t1[1], None]],
          str(a["first_confirmed_product_id"]))
    check("confirmed cell count", a["confirmed_cell_count"] == 4, str(a["confirmed_cell_count"]))
    check("probability line echoed", a["probability_threshold"] == 0.5)

    # exactly two times: a cell qualifies only if both rounds qualify
    s, two = analysis(base_a, [t0[0], t1[0]])
    check("two-time analysis returns only double-qualified cells",
          s == 200 and two["first_confirmed_step"] == [[2, None, None, 2, None]],
          str(two.get("first_confirmed_step")))

    # higher line: p=0.5 no longer qualifies, p=1 does
    s, strict = analysis(base_a, order, threshold=5.0, line=0.75)
    check("higher warning line drops boundary cells",
          strict["first_confirmed_step"] == [[2, 4, 4, None, None]],
          str(strict["first_confirmed_step"]))

    # different threshold selects a different source column in the same product
    s, thr10 = analysis(base_a, order, threshold=10.0, line=0.75)
    check("threshold 10 uses the same products' second threshold",
          thr10["product_ids"] == a["product_ids"] and thr10["rain_threshold"] == 10.0)
    s, bad_thr = analysis(base_a, order, threshold=7.5)
    check("unregistered rain threshold rejected",
          s == 422 and bad_thr["error"]["code"] == "THRESHOLD_MISMATCH", str(bad_thr))

    print("\n[14] cross-time analysis input validation")
    s, d = analysis(base_a, [t0[0]])
    check("single time rejected (need 2-6)", s == 422, str(d))
    s, d = analysis(base_a, [t0[0], t1[0], t2[0], t3[0], t0[0], t1[0], t2[0]])
    check("seven times rejected", s == 422, str(d))
    s, d = analysis(base_a, [t0[0], t1[0], t0[0]])
    check("duplicate group ids rejected", s == 422, str(d))
    s, d = analysis(base_a, [t1[0], t0[0]])
    check("descending time order rejected",
          s == 422 and d["error"]["code"] == "TIME_SEQUENCE_INVALID", str(d))
    s, d = analysis(base_a, [t0[0], t2[0]])
    check("non-hour gap rejected",
          s == 422 and d["error"]["code"] == "TIME_SEQUENCE_INVALID", str(d))
    s, d = analysis(base_a, order, line=1.5)
    check("probability line > 1 rejected", s == 422, str(d))
    s, d = analysis(base_a, order, line=-0.1)
    check("negative probability line rejected", s == 422, str(d))
    s, d = analysis(base_a, order, threshold=-3.0)
    check("negative rain threshold rejected", s == 422, str(d))
    s, d = analysis(base_a, ["nosuch", t1[0]])
    check("unknown time -> 404", s == 404 and d["error"]["code"] == "GROUP_NOT_FOUND", str(d))

    # missing current product (registered + members submitted, never published)
    g_missing = register(base_b, members=2, rows=1, cols=5, thresholds=[5.0],
                         forecast_time="2026-11-16T04:00:00Z")
    for member in ("m1", "m2"):
        http("PUT", base_b, f"/groups/{g_missing['group_id']}/members/{member}",
             {"rain": [[10, 10, 10, 10, 10]], "revision": 1})
    s, d = analysis(base_a, [t3[0], g_missing["group_id"]])
    check("time without a current product is refused",
          s == 409 and d["error"]["code"] == "ANALYSIS_SOURCE_MISSING", str(d))

    # grid mismatch among sources (same hour sequence, different grid)
    gA = register(base_a, members=2, rows=1, cols=2, thresholds=[5.0],
                  forecast_time="2026-11-16T05:00:00Z")
    gB = register(base_a, members=2, rows=1, cols=3, thresholds=[5.0],
                  forecast_time="2026-11-16T06:00:00Z")
    for gid, width in ((gA["group_id"], 2), (gB["group_id"], 3)):
        for member in ("m1", "m2"):
            http("PUT", base_a, f"/groups/{gid}/members/{member}",
                 {"rain": [[10.0] * width], "revision": 1})
        http("POST", base_a, f"/groups/{gid}/publish")
    s, d = analysis(base_a, [gA["group_id"], gB["group_id"]])
    check("grid shape mismatch refused",
          s == 422 and d["error"]["code"] == "GRID_MISMATCH", str(d))

    print("\n[15] analysis is invalidated by concurrent source correction")
    conc_specs = {
        "a": {"time": "2026-11-16T10:00:00Z",
              "m1": [8.0] * 40, "m2": [8.0] * 40},
        "b": {"time": "2026-11-16T11:00:00Z",
              "m1": [8.0] * 40, "m2": [8.0] * 40},
        "c": {"time": "2026-11-16T12:00:00Z",
              "m1": [8.0] * 40, "m2": [8.0] * 40},
    }
    conc = _build_series(base_a, conc_specs, rows=5, cols=8, thresholds=(5.0,))
    conc_ids = [conc[k][0] for k in ("a", "b", "c")]

    result: Dict[str, Any] = {}

    def worker_analysis() -> None:
        result["status"], result["body"] = analysis(base_a, conc_ids)

    th = threading.Thread(target=worker_analysis)
    th.start()
    time.sleep(0.15)  # analysis is now in its (delayed) compute phase
    t0w = time.time()
    s, corr = http("PUT", base_b, f"/groups/{conc['b'][0]}/members/m1",
                   {"rain": [[0.0] * 8 for _ in range(5)], "revision": 2})
    correction_elapsed = time.time() - t0w
    check("correction during analysis accepted", s == 200, str(corr))
    if hooks_enabled:
        check("analysis held no write lock during compute",
              correction_elapsed < 0.5, f"{correction_elapsed:.2f}s")
    th.join()
    check("analysis refuses to deliver the superseded combination",
          result["status"] == 409
          and result["body"]["error"]["code"] == "ANALYSIS_SOURCE_CHANGED", str(result))

    # a different time keeps ingesting while an analysis is computing
    def worker_analysis2() -> None:
        result["status2"], result["body2"] = analysis(base_b, conc_ids)

    s, _ = http("POST", base_a, f"/groups/{conc['b'][0]}/publish")  # restore a current product
    check("republish after correction succeeds", s == 200)
    th2 = threading.Thread(target=worker_analysis2)
    th2.start()
    time.sleep(0.15)
    t0w = time.time()
    g_other = register(base_a, members=2, rows=3, cols=3, thresholds=[5.0],
                       forecast_time="2026-11-16T13:00:00Z")
    s, _ = http("PUT", base_b, f"/groups/{g_other['group_id']}/members/m1",
                {"rain": [[8.0] * 3 for _ in range(3)], "revision": 1})
    s2, _ = http("PUT", base_b, f"/groups/{g_other['group_id']}/members/m2",
                 {"rain": [[8.0] * 3 for _ in range(3)], "revision": 1})
    ingest_elapsed = time.time() - t0w
    check("other time ingests while analysis computes", s == 200 and s2 == 200)
    if hooks_enabled:
        check("other-time ingest stayed fast during analysis",
              ingest_elapsed < 0.5, f"{ingest_elapsed:.2f}s")
    th2.join()
    check("retried analysis succeeds after republish", result["status2"] == 200, str(result))
    s, cur_b = http("GET", base_a, f"/groups/{conc['b'][0]}/products/current")
    check("retried analysis uses the corrected current product",
          result["body2"]["products_used"][1]["product_id"] == cur_b["product_id"],
          f"{result['body2']['products_used'][1]} vs {cur_b['product_id']}")
    check("all cells confirmed across consistent products",
          result["body2"]["confirmed_cell_count"] == 40)

    # correction + immediate republish while an analysis is in flight: the
    # current product id changes, so the in-flight result must be discarded
    def worker_analysis3() -> None:
        result["status3"], result["body3"] = analysis(base_a, conc_ids)

    th3 = threading.Thread(target=worker_analysis3)
    th3.start()
    time.sleep(0.15)
    s, _ = http("PUT", base_b, f"/groups/{conc['a'][0]}/members/m2",
                {"rain": [[9.0] * 8 for _ in range(5)], "revision": 2})
    check("correction accepted mid-analysis (republish case)", s == 200)
    s, new_a = http("POST", base_b, f"/groups/{conc['a'][0]}/publish")
    check("republish mid-analysis succeeds", s == 200)
    th3.join()
    check("analysis invalidated by a republish during compute",
          result["status3"] == 409
          and result["body3"]["error"]["code"] == "ANALYSIS_SOURCE_CHANGED",
          str(result.get("status3")))
    s, final = analysis(base_b, conc_ids)
    check("analysis after the new current products succeeds",
          s == 200 and final["product_ids"][0] == new_a["product_id"], str(final))

    if hooks_enabled:
        print("\n[16] injected compute failure on analysis is reported, nothing persisted")
        before = http("GET", base_a, f"/groups/{conc['a'][0]}/products/current")[1]["product_id"]
        with open(fail_marker, "w") as fh:
            fh.write("fail")
        s, d = analysis(base_a, conc_ids)
        os.remove(fail_marker)
        check("injected failure surfaces as 500",
              s == 500 and d["error"]["code"] == "COMPUTE_FAILED", str(d))
        after = http("GET", base_a, f"/groups/{conc['a'][0]}/products/current")[1]["product_id"]
        check("analysis failure leaves sources untouched", before == after)
    else:
        print("\n[16] SKIP  analysis failure injection (RAIN_TEST_FAIL_MARKER not configured)")


if __name__ == "__main__":
    sys.exit(main())
