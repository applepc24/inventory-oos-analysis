import json
import re
from typing import Any, List, Tuple
from collections import defaultdict

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Referer": "https://www.uniqlo.com/kr/ko/",
}

BASE = "https://www.uniqlo.com"
API_L2S = BASE + "/kr/api/commerce/v5/ko/products/{pid}/price-groups/00/l2s?alterationId=98&withPrices=true&withStocks=true&includePreviousPrice=false&httpFailure=true"

URL = "https://www.uniqlo.com/kr/ko/products/E470549-000/00"

KEYWORDS = ["stock", "avail", "inventory", "quantity", "order", "size", "color", "sku", "status"]

def extract_pid(url: str) -> str:
    m = re.search(r"/products/(E\d+-\d+)", url)
    if not m:
        raise ValueError(f"Cannot parse pid from url: {url}")
    return m.group(1)

def walk(obj: Any, path: str, hits: List[Tuple[str, str, str]]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if any(kw in lk for kw in KEYWORDS):
                if isinstance(v, (str, int, float, bool)) or v is None:
                    preview = str(v)[:120]
                elif isinstance(v, list):
                    preview = f"list(len={len(v)})"
                elif isinstance(v, dict):
                    preview = "dict(keys=" + ",".join(list(v.keys())[:12]) + ")"
                else:
                    preview = type(v).__name__
                hits.append((path, str(k), preview))
            walk(v, f"{path}.{k}", hits)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:200]):
            walk(v, f"{path}[{i}]", hits)

def find_big_record_lists(obj: Any, path: str, out: List[Tuple[str, int, List[str]]]):
    if isinstance(obj, dict):
        for k, v in obj.items():
            find_big_record_lists(v, f"{path}.{k}", out)
    elif isinstance(obj, list):
        if len(obj) >= 10 and all(isinstance(x, dict) for x in obj[:10]):
            common = set(obj[0].keys())
            for x in obj[1:10]:
                common &= set(x.keys())
            out.append((path, len(obj), sorted(list(common))[:30]))
        for i, v in enumerate(obj[:50]):
            find_big_record_lists(v, f"{path}[{i}]", out)

def main():
    pid = extract_pid(URL)
    api = API_L2S.format(pid=pid)
    r = requests.get(api, headers=HEADERS, timeout=30)
    print("STATUS:", r.status_code)
    r.raise_for_status()

    data = r.json()
    with open("l2s_sample.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved: l2s_sample.json")

    hits: List[Tuple[str, str, str]] = []
    walk(data, "$", hits)

    print("\n=== KEYWORD HITS (top 120) ===")
    for row in hits[:120]:
        print(row)

    lists: List[Tuple[str, int, List[str]]] = []
    find_big_record_lists(data, "$", lists)
    lists.sort(key=lambda x: x[1], reverse=True)

    print("\n=== BIG RECORD LISTS (top 20) ===")
    for path, ln, common in lists[:20]:
        print(f"{path}  len={ln}  common_keys={common}")

if __name__ == "__main__":
    main()