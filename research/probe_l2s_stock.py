import json
import re
from collections import Counter
from typing import Any, Dict, List, Tuple

TARGET_HINTS = [
    "stock", "stocks", "inventory", "avail", "available", "orderable",
    "sold", "soldout", "out", "quantity", "qty", "remain", "remaining",
    "purchas", "status"
]

def looks_like_stock_key(k: str) -> bool:
    lk = k.lower()
    return any(h in lk for h in TARGET_HINTS)

def looks_like_stock_value(v: Any) -> bool:
    if isinstance(v, bool):
        return True
    if isinstance(v, (int, float)):
        # 수량/재고는 보통 작은 양의 정수
        return 0 <= v <= 9999
    if isinstance(v, str):
        lv = v.lower()
        return any(x in lv for x in ["in_stock", "out_of_stock", "sold", "unavailable", "available", "orderable"])
    return False

def walk(obj: Any, path: str, key_hits: Counter, val_hits: Counter):
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if looks_like_stock_key(str(k)):
                key_hits[p] += 1
            if looks_like_stock_value(v):
                # 값이 "재고 의미"일 수도 있어서 값 기반 경로도 기록
                val_hits[p] += 1
            walk(v, p, key_hits, val_hits)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:200]):  # 너무 크면 200개까지만
            walk(v, f"{path}[{i}]", key_hits, val_hits)

def simplify_index_paths(counter: Counter) -> Counter:
    """
    $.result.l2s[3].sales[0].stocks[2].quantity -> $.result.l2s[].sales[].stocks[].quantity
    처럼 인덱스를 []로 일반화해서 빈도를 모음
    """
    out = Counter()
    for p, c in counter.items():
        sp = re.sub(r"\[\d+\]", "[]", p)
        out[sp] += c
    return out

def main():
    with open("l2s_sample.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    l2s = data.get("result", {}).get("l2s", [])
    print("l2s rows:", len(l2s))
    if not l2s:
        return

    key_hits = Counter()
    val_hits = Counter()

    # l2s 내부만 집중 탐색
    for i, row in enumerate(l2s[:70]):
        walk(row, "$.result.l2s[%d]" % i, key_hits, val_hits)

    key_hits2 = simplify_index_paths(key_hits)
    val_hits2 = simplify_index_paths(val_hits)

    print("\n=== KEY-BASED STOCK CANDIDATES (top 25) ===")
    for p, c in key_hits2.most_common(25):
        print(c, p)

    print("\n=== VALUE-BASED STOCK CANDIDATES (top 25) ===")
    for p, c in val_hits2.most_common(25):
        print(c, p)

    # 샘플 1개 행의 키 구조도 같이 보여줌(상위 키)
    print("\n=== SAMPLE l2s[0] TOP KEYS ===")
    print(sorted(list(l2s[0].keys())))

    # flags/sales 구조를 빠르게 확인
    if "flags" in l2s[0]:
        print("\nflags keys:", sorted(list(l2s[0]["flags"].keys()))[:50])
    if "sales" in l2s[0]:
        print("\nsales type:", type(l2s[0]["sales"]).__name__)
        if isinstance(l2s[0]["sales"], dict):
            print("sales keys:", sorted(list(l2s[0]["sales"].keys()))[:50])
        if isinstance(l2s[0]["sales"], list) and l2s[0]["sales"]:
            print("sales[0] keys:", sorted(list(l2s[0]["sales"][0].keys()))[:50])

if __name__ == "__main__":
    main()