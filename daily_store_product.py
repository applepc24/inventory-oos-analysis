#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# -----------------------
# 0) .env 자동 로드
# -----------------------
load_dotenv()

# -----------------------
# 1) DB 연결
# -----------------------
def getenv_any(*keys, default=None):
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return v
    return default

def get_engine():
    host = getenv_any("MYSQL_HOST", "DB_HOST")
    port = int(getenv_any("MYSQL_PORT", "DB_PORT", default="3306"))
    user = getenv_any("MYSQL_USER", "DB_USER")
    password = os.getenv("MYSQL_PASSWORD")
    if password is None:
        password = os.getenv("DB_PASSWORD", "")
    db = getenv_any("MYSQL_DB", "DB_NAME")

    if not all([host, user, db]):
        raise RuntimeError(f"Missing env. MYSQL_HOST={host}, MYSQL_USER={user}, MYSQL_DB={db}")

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

# -----------------------
# 2) 일자별 집계 로드 (매장×품번)
#    - v_agg_store_variant_daily가 이미 일자 단위로 쌓여있다고 가정
#    - "매장×품번"으로 합쳐서 그날 하루 전체 품절인지(oos_rate==1) 판단
# -----------------------
def load_daily_store_product(engine, start, end):
    q = text("""
        SELECT
          dt,
          store_name,
          e_code,
          SUM(oos_cnt)   AS oos_cnt,
          SUM(total_cnt) AS total_cnt,
          (SUM(oos_cnt) / NULLIF(SUM(total_cnt), 0)) AS oos_rate
        FROM v_agg_store_variant_daily
        WHERE dt >= :start
          AND dt <  :end
        GROUP BY dt, store_name, e_code
    """)
    df = pd.read_sql(q, engine, params={"start": start, "end": end})
    df["dt"] = pd.to_datetime(df["dt"])
    return df

# -----------------------
# 3) "하루종일 품절" 플래그 + 연속 일수 계산
#    - is_full_oos: oos_rate가 1(또는 거의 1)이면 그날 전체 품절
#    - 각 (store_name, e_code) 그룹에서 날짜 순으로 연속 구간 길이 계산
# -----------------------
def calc_full_oos_streaks(df, rate_threshold=0.999):
    df = df.copy()

    # 안전하게 float 오차 허용
    df["is_full_oos"] = df["oos_rate"].fillna(0) >= rate_threshold

    # 그룹별 날짜 정렬
    df = df.sort_values(["store_name", "e_code", "dt"])

    group_cols = ["store_name", "e_code"]

    # 연속 구간(run) 만들기:
    # - is_full_oos 값이 바뀌는 지점마다 run_id 증가
    df["run_id"] = (
        df.groupby(group_cols)["is_full_oos"]
          .apply(lambda s: (s != s.shift(1)).cumsum())
          .reset_index(level=group_cols, drop=True)
    )

    # run 단위로 길이/구간 계산
    runs = (
        df.groupby(group_cols + ["run_id"], as_index=False)
          .agg(
              is_full_oos=("is_full_oos", "first"),
              start_dt=("dt", "min"),
              end_dt=("dt", "max"),
              days=("dt", "size"),
          )
    )

    # "하루종일 품절" run만 남기기
    full_runs = runs[runs["is_full_oos"]].copy()

    # 각 (매장, 품번)별 최대 연속일수 + 그 구간
    summary = (
        full_runs.sort_values(["store_name", "e_code", "days"], ascending=[True, True, False])
                .groupby(group_cols, as_index=False)
                .head(1)
                .rename(columns={
                    "days": "max_full_oos_streak_days",
                    "start_dt": "max_streak_start_dt",
                    "end_dt": "max_streak_end_dt",
                })
                .drop(columns=["run_id", "is_full_oos"])
    )

    # 참고용: 전체 기간 중 '하루종일 품절' 일수 합계도 같이
    total_full_days = (
        df[df["is_full_oos"]]
        .groupby(group_cols, as_index=False)
        .agg(total_full_oos_days=("is_full_oos", "size"))
    )

    summary = summary.merge(total_full_days, on=group_cols, how="left").fillna({"total_full_oos_days": 0})
    summary["total_full_oos_days"] = summary["total_full_oos_days"].astype(int)
    summary["max_full_oos_streak_days"] = summary["max_full_oos_streak_days"].astype(int)

    return df, runs, summary

# -----------------------
# 4) 저장 + TOP 보기
# -----------------------
def save_outputs(df_daily, runs, summary, outdir="out/oos_persistence"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_daily.to_csv(outdir / "daily_store_product.csv", index=False, encoding="utf-8-sig")
    runs.to_csv(outdir / "runs_store_product.csv", index=False, encoding="utf-8-sig")
    summary.to_csv(outdir / "summary_store_product_maxstreak.csv", index=False, encoding="utf-8-sig")
    print(f"[SAVE] {outdir}")

def main():
    start = os.getenv("VIZ_START", "2025-12-29")
    end   = os.getenv("VIZ_END",   "2026-01-03")

    engine = get_engine()

    # 1) 일자별 매장×품번 데이터
    df_daily = load_daily_store_product(engine, start, end)

    # 2) 연속 '하루종일 품절' 계산
    df_daily2, runs, summary = calc_full_oos_streaks(df_daily, rate_threshold=0.999)

    # 3) 저장
    save_outputs(df_daily2, runs, summary)

    # 4) 화면에 TOP 20 (연속 품절이 긴 순)
    top = summary.sort_values(["max_full_oos_streak_days", "total_full_oos_days"], ascending=False).head(20)
    print("\n[TOP 20] 매장×품번 최대 연속 '하루종일 품절' 일수")
    print(top.to_string(index=False))

if __name__ == "__main__":
    main()