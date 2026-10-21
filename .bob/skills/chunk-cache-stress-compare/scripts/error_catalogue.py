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
# Group the ERROR/WARN entries of a run's system.log (rotated zips included, in order) into distinct errors.
# usage: error_catalogue.py <run dir> <out txt> <out tsv>
import glob, os, re, sys, zipfile

run, out_txt, out_tsv = sys.argv[1:4]
logs = os.path.join(run, "logs")

def lines():
    zips = sorted(glob.glob(os.path.join(logs, "system.log.*.zip")), key=lambda p: int(p.rsplit(".", 2)[-2]))
    for z in zips:
        with zipfile.ZipFile(z) as zf:
            for n in zf.namelist():
                with zf.open(n) as f:
                    for l in f:
                        yield l.decode("utf-8", "replace").rstrip("\n")
    with open(os.path.join(logs, "system.log"), encoding="utf-8", errors="replace") as f:
        for l in f:
            yield l.rstrip("\n")

HDR = re.compile(r"^(ERROR|WARN|INFO|DEBUG|TRACE)\s+\[([^\]]+)\]\s+(\S+)\s+(\S+\.java:\d+) - (.*)$")

def norm(s):
    s = re.sub(r"0x[0-9a-f]+", "0x?", s)
    s = re.sub(r"\d+(\.\d+)?", "N", s)
    return s

def pool(t):
    return re.sub(r"[:-]\d+$", "", t)

groups = {}
order = []

def flush(e):
    if e is None or e["level"] not in ("ERROR", "WARN"):
        return
    body = e["body"]
    exc = next((b for b in body if not b.startswith("\t") and not b.startswith(" ")), "")
    frames = [b.strip() for b in body if b.strip().startswith("at org.apache.cassandra")]
    # first frames below the chunk cache give the caller; keep a short signature
    sig_frames = [f for f in frames if "ChunkCache" not in f and "io.util" not in f][:4]
    key = (e["level"], pool(e["thread"]), e["src"], norm(e["msg"])[:120], norm(exc.split(":")[0]), tuple(sig_frames))
    g = groups.get(key)
    if g is None:
        g = groups[key] = {"count": 0, "first": e["ts"], "last": e["ts"], "sample": [e["raw"]] + body}
        order.append(key)
    g["count"] += 1
    g["last"] = e["ts"]

cur = None
for l in lines():
    m = HDR.match(l)
    if m:
        flush(cur)
        cur = {"level": m.group(1), "thread": m.group(2), "ts": m.group(3), "src": m.group(4), "msg": m.group(5), "raw": l, "body": []}
    elif cur is not None:
        cur["body"].append(l)
flush(cur)

order.sort(key=lambda k: -groups[k]["count"])
with open(out_tsv, "w") as t, open(out_txt, "w") as o:
    t.write("id\tcount\tlevel\tthread_pool\tsource\tfirst\tlast\texception\tmessage\n")
    for i, k in enumerate(order, 1):
        g = groups[k]
        exc = next((b for b in g["sample"][1:] if not b.startswith("\t")), "")
        t.write(f"E{i}\t{g['count']}\t{k[0]}\t{k[1]}\t{k[2]}\t{g['first']}\t{g['last']}\t{exc[:160]}\t{k[3][:120]}\n")
        o.write(f"===== E{i}: {g['count']} occurrence(s), {k[0]} in {k[1]} ({k[2]}), first {g['first']}, last {g['last']}\n")
        o.write("\n".join(g["sample"][:120]))
        o.write("\n\n")
