import re,sys,os,collections
pat=re.compile(r"CSWEEP state=(\w+) segments=\[([^\]]*)\] limit=(\d+) rerank_k=(\w+) recall=([\d.]+) qps=(\d+)")
def load(f):
    d=collections.defaultdict(dict)
    for line in open(f):
        m=pat.search(line)
        if m:
            st,segs,L,k,r,q=m.groups(); d[(st,len(segs.split()))].setdefault(int(L),[]).append((k,float(r)))
    return d
print("file                      state  segs | knee k (multiple) per LIMIT 1,5,10,20,50,100 | plateau@10 default@10")
for f in sys.argv[1:]:
    d=load(f)
    for (st,ns),byL in sorted(d.items()):
        cells=[]; p10=d10=None
        for L in [1,5,10,20,50,100]:
            rows=byL.get(L)
            if not rows: cells.append("   -    "); continue
            num=sorted((int(k),r) for k,r in rows if k!='default'); dflt=[r for k,r in rows if k=='default'][0]
            pl=max(r for _,r in num); knee=next(k for k,r in num if r>=pl-0.01)
            cells.append(f"{knee:4d}({knee/L:4.1f})")
            if L==10: p10,d10=pl,dflt
        print(f"{os.path.basename(f)[:-4]:25s} {st[:5]:5s} {ns:4d} | {' '.join(cells)} | {p10} {d10}")
