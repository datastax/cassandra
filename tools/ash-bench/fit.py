import re,sys,os,collections
import numpy as np
pat=re.compile(r"CSWEEP state=(\w+) segments=\[([^\]]*)\] limit=(\d+) rerank_k=(\w+) recall=([\d.]+) qps=(\d+)")
def curves(f,state="compacted"):
    d={}
    for line in open(f):
        m=pat.search(line)
        if m and m.group(1)==state and m.group(4)!='default':
            d.setdefault(int(m.group(3)),{})[int(m.group(4))]=float(m.group(5))
    return d
def knee(c,margin=0.01):
    pl=max(c.values()); return min(k for k,r in c.items() if r>=pl-margin), pl
def interp(c,k):
    ks=sorted(c); 
    if k<=ks[0]: return c[ks[0]]
    for a,b in zip(ks,ks[1:]):
        if a<=k<=b: return c[a]+(c[b]-c[a])*(k-a)/(b-a)
    return c[ks[-1]]
files=sys.argv[1:]
print("dataset            knees L=1,5,10,20,50,100     affine fit a + b*L")
allpts=[]
for f in files:
    d=curves(f); Ls=sorted(d)
    ks=[knee(d[L])[0] for L in Ls]
    b,a=np.polyfit(Ls,ks,1)
    print(f"{os.path.basename(f)[:-4]:18s} {ks}  k = {a:5.0f} + {b:4.2f} L")
    allpts.append((os.path.basename(f)[:-4],d))
