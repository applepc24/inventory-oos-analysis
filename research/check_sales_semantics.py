import json
from collections import Counter

with open("l2s_sample.json", "r", encoding="utf-8") as f:
    data = json.load(f)

l2s = data["result"]["l2s"]
c = Counter([row.get("sales") for row in l2s])
print("sales distribution:", c)

# sales=False 샘플 5개만 보기
false_rows = [r for r in l2s if r.get("sales") is False]
print("sales=False rows:", len(false_rows))

for i, r in enumerate(false_rows[:5], 1):
    color = (r.get("color") or {}).get("displayCode")
    size  = (r.get("size") or {}).get("displayCode")
    l2id  = r.get("l2Id")
    flags = r.get("flags") or {}
    pf = flags.get("productFlags") or []
    print(f"\n[{i}] l2Id={l2id} color={color} size={size} sales={r.get('sales')}")
    print("productFlags:", pf)