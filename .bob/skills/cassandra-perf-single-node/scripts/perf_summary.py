#!/usr/bin/env python3
# Copyright IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Markdown summary of one perf.sh run, optionally compared with a baseline run.
#
# usage: perf_summary.py <results/<label>> [--baseline <results/<label>>]
#
# Latency comes from the nb5 timer csv (--report-csv-to): result_success for every workload except lwt, where
# 'result' (all ops) is used because rejected conditions are normal results. nb5 writes one row per report interval
# with per-interval counts and percentiles: counts are summed, percentiles are count-weighted over the intervals
# (the highest interval p99 is also reported). Throughput = successful ops / step duration.
import csv
import glob
import json
import os
import sys

LATENCY_METRIC = {"lwt": "result"}
UNIT_TO_MS = {"NANOSECONDS": 1e-6, "MICROSECONDS": 1e-3, "MILLISECONDS": 1.0, "SECONDS": 1000.0}
STEP_ORDER = ["rampup", "main-read", "main-write", "main-mixed", "main", "main-eq", "main-range", "main-ann"]


def rows_of(path):
    try:
        with open(path, newline="") as fh:
            out = []
            for r in csv.DictReader(fh):
                d = {}
                for k, v in r.items():
                    try:
                        d[k] = float(v)
                    except (TypeError, ValueError):
                        d[k] = v
                out.append(d)
            return out
    except OSError:
        return []


def metric_path(csv_dir, metric):
    p = glob.glob(os.path.join(csv_dir, "*__%s_container_default.csv" % metric))
    return p[0] if p else None


def timer(path):
    rows = rows_of(path) if path else []
    if not rows:
        return None
    f = UNIT_TO_MS.get(str(rows[-1].get("duration_unit", "NANOSECONDS")).upper(), 1e-6)

    def weighted(k):
        num = den = 0.0
        for r in rows:
            c, v = r.get("count"), r.get(k)
            if isinstance(c, float) and isinstance(v, float) and c > 0:
                num += c * v
                den += c
        return num / den * f if den else None

    p99s = [r["p99"] for r in rows if isinstance(r.get("p99"), float)]
    maxs = [r["max"] for r in rows if isinstance(r.get("max"), float)]
    return {"count": sum(r["count"] for r in rows if isinstance(r.get("count"), float)),
            "p50": weighted("p50"), "p99": weighted("p99"), "p999": weighted("p999"),
            "p99_max": max(p99s) * f if p99s else None, "max": max(maxs) * f if maxs else None}


def last_value(path):
    rows = rows_of(path) if path else []
    if not rows:
        return None
    r = rows[-1]
    for k in ("value", "count"):
        if isinstance(r.get(k), float):
            return r[k]
    return None


def parse(root):
    out = {}
    for wl in sorted(os.listdir(root)):
        wdir = os.path.join(root, wl)
        if not os.path.isdir(wdir):
            continue
        for st in os.listdir(wdir):
            sdir = os.path.join(wdir, st)
            if st == "schema" or not os.path.isfile(os.path.join(sdir, "run.json")):
                continue
            with open(os.path.join(sdir, "run.json")) as fh:
                run = json.load(fh)
            csv_dir = os.path.join(sdir, "csv")
            lat = timer(metric_path(csv_dir, LATENCY_METRIC.get(wl, "result_success")))
            succ = timer(metric_path(csv_dir, "result_success"))
            errors = last_value(metric_path(csv_dir, "errors_total"))
            unapplied = last_value(metric_path(csv_dir, "errors_ChangeUnappliedCycleException"))
            if errors is not None and unapplied:
                errors -= unapplied
            dur = run.get("duration_s") or 0
            ok = succ["count"] if succ else None
            out[(wl, st)] = {"duration_s": dur, "exit_code": run.get("exit_code"), "lat": lat, "ok": ok,
                             "tput": ok / dur if ok and dur else None, "errors": errors, "unapplied": unapplied}
        cnt = os.path.join(wdir, "server-errors.count")
        if os.path.exists(cnt):
            out[(wl, "_server_errors")] = int(open(cnt).read().strip() or 0)
    return out


def fmt(v, nd=2):
    return "-" if v is None else ("%.*f" % (nd, v))


def delta(new, base):
    if new is None or base is None or base == 0:
        return "-"
    return "%+.1f%%" % ((new - base) / base * 100.0)


def order(keys):
    def k(x):
        wl, st = x
        return (wl, STEP_ORDER.index(st) if st in STEP_ORDER else 99, st)
    return sorted([x for x in keys if not x[1].startswith("_")], key=k)


def params(root):
    p = os.path.join(root, "params.txt")
    return open(p).read().strip() if os.path.exists(p) else ""


def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print("usage: perf_summary.py <results/<label>> [--baseline <results/<label>>]")
        return 64
    root = argv[0]
    base_root = argv[2] if len(argv) >= 3 and argv[1] == "--baseline" else None
    cur = parse(root)
    base = parse(base_root) if base_root else {}
    print("# Perf summary: %s" % os.path.basename(os.path.normpath(root)))
    print("\n```\n%s\n```" % params(root))
    if base_root:
        print("\nBaseline: `%s`\n\n```\n%s\n```" % (os.path.basename(os.path.normpath(base_root)), params(base_root)))
    print("\nLatency in ms (nb5 `result_success` timer; `result` for lwt), count-weighted over the 10 s report intervals. "
          "Throughput = successful ops / step duration.\n")
    if base_root:
        print("| workload | step | ops/s | Δ ops/s | p50 | Δ p50 | p99 | Δ p99 | p99.9 | Δ p99.9 | max p99 | errors (base) | exit |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    else:
        print("| workload | step | duration s | ok ops | ops/s | p50 | p99 | p99.9 | max p99 | max | errors | exit |")
        print("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for key in order(cur.keys()):
        s = cur[key]
        lat = s["lat"] or {}
        if base_root:
            b = base.get(key) or {}
            bl = b.get("lat") or {}
            print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s (%s) | %s |" % (
                key[0], key[1], fmt(s["tput"], 0), delta(s["tput"], b.get("tput")),
                fmt(lat.get("p50")), delta(lat.get("p50"), bl.get("p50")),
                fmt(lat.get("p99")), delta(lat.get("p99"), bl.get("p99")),
                fmt(lat.get("p999")), delta(lat.get("p999"), bl.get("p999")),
                fmt(lat.get("p99_max")), fmt(s["errors"], 0), fmt(b.get("errors"), 0), s["exit_code"]))
        else:
            print("| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |" % (
                key[0], key[1], s["duration_s"], fmt(s["ok"], 0), fmt(s["tput"], 0), fmt(lat.get("p50")),
                fmt(lat.get("p99")), fmt(lat.get("p999")), fmt(lat.get("p99_max")), fmt(lat.get("max")),
                fmt(s["errors"], 0), s["exit_code"]))
    srv = {k[0]: v for k, v in cur.items() if k[1] == "_server_errors"}
    if srv:
        print("\nServer ERROR log lines per workload: " + ", ".join("%s %d" % kv for kv in sorted(srv.items())))
    unap = {k: v["unapplied"] for k, v in cur.items() if not k[1].startswith("_") and v.get("unapplied")}
    if unap:
        print("\nLWT conditions not applied (expected by the workload design, not errors): " +
              ", ".join("%s/%s %d" % (k[0], k[1], v) for k, v in sorted(unap.items())))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
