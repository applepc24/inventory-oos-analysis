#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import requests
import pymysql


# -----------------------------
# .env 로드 (있으면 로드, 없어도 OK)
# -----------------------------
def load_env():
    try:
        from dotenv import load_dotenv  # pip install python-dotenv
        load_dotenv()
    except Exception:
        pass


load_env()


# -----------------------------
# Env helpers
# -----------------------------
def env(name: str, default: Optional[str] = None) -> str:
    v = os.environ.get(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing env var: {name}")
    return v


def mysql_conn():
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "retail_portfolio"),
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


# -----------------------------
# Uniqlo API
# -----------------------------
UNIQLO_L2S_URL_TMPL = "https://www.uniqlo.com/kr/api/commerce/v5/ko/products/{e_code}/price-groups/00/l2s"

DEFAULT_PARAMS_BASE = {
    "withPrices": "true",
    "withStocks": "false",  # dim 채우기라면 굳이 재고까지 필요 없음
    "includePreviousPrice": "false",
    "httpFailure": "true",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; data-collector/1.0; +local)",
    "Accept": "application/json",
}


def fetch_l2s(e_code: str, alteration_id: Optional[int] = 98) -> dict:
    """
    alterationId=98(바지 수선) 붙이면 잘 나오는 케이스가 많아서 1차 시도.
    실패하면 alterationId 없이도 재시도.
    """
    url = UNIQLO_L2S_URL_TMPL.format(e_code=e_code)

    def _do(params: dict) -> dict:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # 1) alterationId 포함 시도
    if alteration_id is not None:
        params = dict(DEFAULT_PARAMS_BASE)
        params["alterationId"] = str(alteration_id)
        try:
            return _do(params)
        except Exception:
            pass

    # 2) alterationId 없이 시도
    params = dict(DEFAULT_PARAMS_BASE)
    return _do(params)


def parse_l2_map(api_json: dict) -> Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    returns: { l2Id: (color_display, size_display, communication_code) }
    """
    result = api_json.get("result") or {}
    l2s = result.get("l2s") or []

    m: Dict[str, Tuple[Optional[str], Optional[str], Optional[str]]] = {}
    for item in l2s:
        l2_id = item.get("l2Id")
        if not l2_id:
            continue

        color = item.get("color") or {}
        size = item.get("size") or {}

        # displayCode가 "01", "002" 같은 코드로 내려오는 걸 확인했지.
        color_display = color.get("displayCode")  # 예: "01"
        size_display = size.get("displayCode")    # 예: "002"
        communication_code = item.get("communicationCode")  # 예: "483722-01-002-000"

        m[str(l2_id)] = (
            str(color_display) if color_display is not None else None,
            str(size_display) if size_display is not None else None,
            str(communication_code) if communication_code is not None else None,
        )
    return m


# -----------------------------
# DDL (옵션)
# -----------------------------
DDL_PRODUCT_L2_DIM = """
CREATE TABLE IF NOT EXISTS product_l2_dim (
  e_code             VARCHAR(32)  NOT NULL,
  l2_id              VARCHAR(16)  NOT NULL,
  color_display      VARCHAR(32)  NULL,
  size_display       VARCHAR(32)  NULL,
  communication_code VARCHAR(64)  NULL,
  first_seen         DATETIME     NOT NULL,
  last_seen          DATETIME     NOT NULL,
  PRIMARY KEY (e_code, l2_id),
  INDEX idx_e_code (e_code),
  INDEX idx_l2_id (l2_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def init_tables():
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_PRODUCT_L2_DIM)
    finally:
        conn.close()


# -----------------------------
# DB ops
# -----------------------------
def fetch_e_codes_from_log(start: str, end: str) -> List[str]:
    sql = """
    SELECT DISTINCT e_code
    FROM store_stock_log
    WHERE collected_at >= %s AND collected_at < %s
    ORDER BY e_code
    """
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start, end))
            rows = cur.fetchall()
            return [r["e_code"] for r in rows]
    finally:
        conn.close()


def fetch_l2_ids_for_e_code(e_code: str, start: str, end: str) -> List[str]:
    """
    로그에 실제로 등장한 l2_id만 dim에 채우고 싶으면 이걸 사용.
    (API에는 훨씬 많은 l2가 있을 수 있음)
    """
    sql = """
    SELECT DISTINCT l2_id
    FROM store_stock_log
    WHERE e_code = %s
      AND collected_at >= %s AND collected_at < %s
    ORDER BY l2_id
    """
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (e_code, start, end))
            rows = cur.fetchall()
            return [r["l2_id"] for r in rows]
    finally:
        conn.close()


def upsert_product_l2_dim(
    e_code: str,
    l2_rows: List[Tuple[str, str, Optional[str], Optional[str], Optional[str], str, str]],
) -> int:
    """
    l2_rows row schema:
    (e_code, l2_id, color_display, size_display, communication_code, first_seen, last_seen)
    """
    if not l2_rows:
        return 0

    sql = """
    INSERT INTO product_l2_dim
      (e_code, l2_id, color_display, size_display, communication_code, first_seen, last_seen)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      color_display      = COALESCE(VALUES(color_display), color_display),
      size_display       = COALESCE(VALUES(size_display), size_display),
      communication_code = COALESCE(VALUES(communication_code), communication_code),
      last_seen          = VALUES(last_seen)
    """
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, l2_rows)
        return len(l2_rows)
    finally:
        conn.close()


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD or YYYY-MM-DD HH:MM:SS (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD or YYYY-MM-DD HH:MM:SS (exclusive)")
    ap.add_argument("--init-ddl", action="store_true", help="Create product_l2_dim if not exists")
    ap.add_argument("--alteration-id", type=int, default=98, help="uniqlo alterationId to try first (default: 98)")
    ap.add_argument("--all-api-l2s", action="store_true",
                    help="If set, insert ALL l2s returned by API (not only l2_ids seen in store_stock_log)")
    args = ap.parse_args()

    # env로만 연결하고 싶다 했으니, 여기서 최소한 DB env만 확인
    # (없으면 기본값들로 연결 시도하게 되어 있음)
    _ = os.environ.get("MYSQL_HOST", "127.0.0.1")

    if args.init_ddl:
        init_tables()
        print("[INIT] product_l2_dim ensured.")

    e_codes = fetch_e_codes_from_log(args.start, args.end)
    if not e_codes:
        print("[WARN] No e_code found in store_stock_log for given range.")
        return

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_upsert = 0

    for e_code in e_codes:
        print(f"[ECODE] {e_code} ...")

        try:
            api_json = fetch_l2s(e_code, alteration_id=args.alteration_id)
        except Exception as ex:
            print(f"  [FAIL] API fetch failed: {ex}")
            continue

        l2_map = parse_l2_map(api_json)
        if not l2_map:
            print("  [WARN] API returned no l2s")
            continue

        if args.all_api_l2s:
            target_l2_ids = list(l2_map.keys())
        else:
            # 로그에 실제 등장한 l2_id만 dim에 채우기(너희 분석 취지에 더 잘 맞음)
            target_l2_ids = fetch_l2_ids_for_e_code(e_code, args.start, args.end)

        rows_to_upsert: List[Tuple[str, str, Optional[str], Optional[str], Optional[str], str, str]] = []
        for l2_id in target_l2_ids:
            color_display, size_display, comm = l2_map.get(str(l2_id), (None, None, None))
            rows_to_upsert.append((e_code, str(l2_id), color_display, size_display, comm, now, now))

        n = upsert_product_l2_dim(e_code, rows_to_upsert)
        total_upsert += n
        print(f"  -> upserted: {n} rows (targets={len(target_l2_ids)}, api_l2s={len(l2_map)})")

    print("DONE")
    print("total_upserted:", total_upsert)
    print("finished_at:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == "__main__":
    main()