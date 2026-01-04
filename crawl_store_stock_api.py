import os
import json
import time
from datetime import datetime
from urllib.parse import quote

import requests
import pymysql
from fetch_l2_ids import fetch_l2_ids

BASE = "https://www.uniqlo.com/kr/api/commerce/v5/ko"

# ✅ 최소 헤더만 (쿠키 하드코딩 금지)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    # 아래 2개는 있으면 안정적인 편 (없어도 되는 경우 많음)
    "x-fr-clientid": "uq.kr.web-spa",
}

SCORE = {
    "OUT_OF_STOCK": 0,
    "LOW_STOCK": 1,
}

def status_to_score(status: str) -> int:
    return SCORE.get(status, 2)

def fetch_stores(l2_id: str, keyword: str, limit: int = 5) -> dict:
    url = f"{BASE}/l2s/{l2_id}/stores?keyword={quote(keyword)}&unit=km&priceGroup=00&limit={limit}&httpFailure=true"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def insert_rows(rows):
    if not rows:
        return 0

    conn = pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "retail_portfolio"),
        charset="utf8mb4",
        autocommit=True,
    )

    sql = """
    INSERT INTO store_stock_log
      (collected_at, run_id, e_code, l2_id, keyword, store_id, store_name, store_type_code, stock_status, availability_score)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    conn.close()
    return len(rows)

def upsert_run_best(run_id: str) -> None:
    conn = pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "retail_portfolio"),
        charset="utf8mb4",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("CALL sp_upsert_store_stock_run_best(%s)", (run_id,))
    conn.close()


def load_active_e_codes():
    conn = pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "retail_portfolio"),
        charset="utf8mb4",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT e_code
            FROM watched_product
            WHERE is_active=1
            ORDER BY created_at ASC
        """)
        rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def main():
    keywords = ["강남", "용산", "롯데월드몰", "롯데백화점 본점", "송도"]
    limit = 10

    now = datetime.now()
    collected_at = now.strftime("%Y-%m-%d %H:%M:%S")
    run_id = now.strftime("%Y%m%d%H%M%S")

    E_CODES = load_active_e_codes()
    print(f"ACTIVE E_CODES: {len(E_CODES)}")
    if not E_CODES:
        print("No active e_codes. Check watched_product table.")
        return

    all_rows = []

    for e_code in E_CODES:
        l2_ids = fetch_l2_ids(e_code)
        l2_ids = l2_ids[:2]
        print(f"{e_code} → l2_ids: {l2_ids}")

        for l2_id in l2_ids:
            for kw in keywords:
                print(f"GET l2={l2_id} keyword={kw}")

                try:
                    data = fetch_stores(l2_id, kw, limit=limit)
                except requests.exceptions.RequestException as e:
                    print(f"FAILED l2={l2_id} kw={kw} err={e}")
                    continue

                stores = data.get("result", {}).get("stores", [])
                if not stores:
                    print(f"  -> no stores (l2={l2_id}, kw={kw})")
                    time.sleep(0.2)
                    continue

                for s in stores:
                    stock_status = s.get("stockStatus", "UNKNOWN")
                    row = (
                        collected_at,
                        run_id,
                        e_code,
                        l2_id,
                        kw,
                        str(s.get("storeId", "")),
                        s.get("storeName"),
                        s.get("storeTypeCode"),
                        stock_status,
                        status_to_score(stock_status),
                    )
                    all_rows.append(row)

                time.sleep(0.6)

    n = insert_rows(all_rows)
    upsert_run_best(run_id)

    print(f"run_id: {run_id}")
    print(f"Collected_at: {collected_at}")
    print(f"Inserted rows: {n}")

if __name__ == "__main__":
    main()