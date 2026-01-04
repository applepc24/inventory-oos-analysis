import json
import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple

import requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Referer": "https://www.uniqlo.com/kr/ko/",
}

BASE = "https://www.uniqlo.com"
API_DETAILS = BASE + "/kr/api/commerce/v5/ko/products/{pid}/price-groups/00/details?includeModelSize=true&imageRatio=3x4&httpFailure=true"

URL = "https://www.uniqlo.com/kr/ko/products/E470549-000/00"

def extract_pid(url: str) -> str:
    m = re.search(r"/products/(E\d+-\d+)", url)
    if not m:
        raise ValueError(f"Cannot parse pid from url: {url}")
    return m.group(1)

KEYWORDS = ["color", "size", "sku", "stock", "avail", "inventory", "status", "style"]

def walk(obj: Any, path: str, hits: List[Tuple[str, str, str]]):
    """
    hits: (path, key, preview)
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if any(kw in lk for kw in KEYWORDS):
                preview = ""
                try:
                    if isinstance(v, (str, int, float, bool)) or v is None:
                        preview = str(v)[:120]
                    elif isinstance(v, list):
                        preview = f"list(len={len(v)})"
                    elif isinstance(v, dict):
                        preview = "dict(keys=" + ",".join(list(v.keys())[:8]) + ")"
                except Exception:
                    preview = "<?>"
                hits.append((path, str(k), preview))
            walk(v, f"{path}.{k}", hits)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:200]):  # 너무 길면 앞 200개만
            walk(v, f"{path}[{i}]", hits)

def find_repeating_records(obj: Any, path: str, candidates: List[Tuple[str, int, List[str]]]):
    """
    배열 중에 '레코드 리스트'처럼 보이는 애들을 찾는다.
    (리스트 길이가 크고, 원소가 dict이며, 공통 키가 많은 경우)
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            find_repeating_records(v, f"{path}.{k}", candidates)
    elif isinstance(obj, list):
        if len(obj) >= 10 and all(isinstance(x, dict) for x in obj[:10]):
            # 공통 키 추정
            common = set(obj[0].keys())
            for x in obj[1:10]:
                common &= set(x.keys())
            candidates.append((path, len(obj), sorted(list(common))[:20]))
        for i, v in enumerate(obj[:50]):
            find_repeating_records(v, f"{path}[{i}]", candidates)

def main():
    pid = extract_pid(URL)
    api = API_DETAILS.format(pid=pid)
    r = requests.get(api, headers=HEADERS, timeout=30)
    print("STATUS:", r.status_code)
    r.raise_for_status()

    data = r.json()

    # 1) raw 저장
    with open("details_sample.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("Saved: details_sample.json")

    # 2) 키워드 히트
    hits: List[Tuple[str, str, str]] = []
    walk(data, "$", hits)
    print("\n=== KEYWORD HITS (top 80) ===")
    for row in hits[:80]:
        print(row)

    # 3) 레코드 배열 후보
    candidates: List[Tuple[str, int, List[str]]] = []
    find_repeating_records(data, "$", candidates)
    candidates.sort(key=lambda x: x[1], reverse=True)

    print("\n=== REPEATING RECORD LIST CANDIDATES (top 30) ===")
    for path, ln, common_keys in candidates[:30]:
        print(f"{path}  len={ln}  common_keys={common_keys}")

if __name__ == "__main__":
    main()