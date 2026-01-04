# fetch_l2_ids.py
from typing import List
import requests

BASE = "https://www.uniqlo.com/kr/api/commerce/v5/ko"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "x-fr-clientid": "uq.kr.web-spa",
}

def fetch_l2_ids(e_code: str) -> List[str]:
    url = (
        f"{BASE}/products/{e_code}/price-groups/00/l2s"
        f"?alterationId=98&withPrices=true&withStocks=true&includePreviousPrice=false&httpFailure=true"
    )
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()

    l2s = data.get("result", {}).get("l2s", [])
    # ✅ 진짜 l2Id(보통 8자리 숫자 문자열)만 추출
    ids = []
    for item in l2s:
        l2_id = item.get("l2Id")
        if l2_id:
            ids.append(str(l2_id))

    # 중복 제거 + 순서 유지
    seen = set()
    uniq = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            uniq.append(x)

    return uniq