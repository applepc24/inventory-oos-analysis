#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm

import seaborn as sns
from sqlalchemy import create_engine, text


# -----------------------
# 0) .env 자동 로드
# -----------------------
load_dotenv()  # 현재 디렉토리의 .env 자동 로드


# -----------------------
# 1) 한글 폰트 세팅 (네모 깨짐 해결)
# -----------------------
def setup_korean_font():
    """
    ✅ macOS에서 한글 네모(□) 깨짐 방지용 확실한 세팅
    - Matplotlib 기본 폰트(Arial 등)로 튀는 걸 막기 위해
      font.family를 'sans-serif'로 두고,
      font.sans-serif 리스트에 한글 폰트를 1순위로 고정한다.
    """
    candidates = [
        "Apple SD Gothic Neo",
        "AppleGothic",
        "NanumGothic",
        "Noto Sans CJK KR",
        "Noto Sans KR",
        "Malgun Gothic",
        "맑은 고딕",
    ]

    installed = {f.name for f in fm.fontManager.ttflist}
    chosen = next((name for name in candidates if name in installed), None)

    if not chosen:
        print("[FONT] 한글 폰트를 못 찾았어요. (AppleGothic/Nanum/Noto 설치 필요)")
        chosen = "DejaVu Sans"  # 마지막 fallback (한글은 보장 X)

    # ✅ 핵심: family를 sans-serif로 두고, sans-serif 후보 맨 앞에 chosen을 넣는다
    mpl.rcParams["font.family"] = "sans-serif"
    mpl.rcParams["font.sans-serif"] = [chosen]
    mpl.rcParams["axes.unicode_minus"] = False

    # seaborn에도 폰트까지 같이 박아주기
    sns.set_theme(style="whitegrid", font=chosen)

    print(f"[FONT] chosen = {chosen}")
    print(f"[FONT] rcParams font.family = {mpl.rcParams['font.family']}")
    print(f"[FONT] rcParams font.sans-serif = {mpl.rcParams['font.sans-serif']}")


# -----------------------
# 2) ENV 헬퍼 + DB 연결
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
    db = getenv_any("MYSQL_DB", "DB_NAME", "DB_DB")

    if not all([host, user, db]):
        raise RuntimeError(
            f"Missing MySQL env vars. MYSQL_HOST={host}, MYSQL_USER={user}, MYSQL_DB={db}. "
            f"(.env 또는 export로 MYSQL_HOST, MYSQL_USER, MYSQL_DB 설정 필요)"
        )

    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)


# -----------------------
# 3) 데이터 로딩
# -----------------------
def load_store_size_heatmap_data(engine, e_code, start, end):
    q = text("""
        SELECT
          store_name,
          size_display,
          SUM(oos_cnt) AS oos_cnt,
          SUM(total_cnt) AS total_cnt,
          (SUM(oos_cnt) / NULLIF(SUM(total_cnt), 0)) AS oos_rate
        FROM v_agg_store_variant_daily
        WHERE e_code = :e_code
          AND dt >= :start
          AND dt <  :end
        GROUP BY store_name, size_display
    """)
    return pd.read_sql(q, engine, params={"e_code": e_code, "start": start, "end": end})


def load_product_scatter_data(engine, start, end):
    """
    제품(품번)별:
    - mean_oos_rate: 평균 품절 비율
    - std_oos_rate : 매장 간 품절 편차(표준편차)
    - variant_cnt  : 변형(사이즈/옵션) 개수
    - obs_cnt      : 관측치(집계 row 수) -> "점이 왜 적지?" 디버깅에도 도움
    """
    q = text("""
        SELECT
          e_code,
          AVG(oos_rate) AS mean_oos_rate,
          STDDEV_POP(oos_rate) AS std_oos_rate,
          COUNT(DISTINCT l2_id) AS variant_cnt,
          COUNT(*) AS obs_cnt
        FROM v_agg_store_variant_daily
        WHERE dt >= :start
          AND dt <  :end
        GROUP BY e_code
    """)
    df = pd.read_sql(q, engine, params={"start": start, "end": end})
    df["std_oos_rate"] = df["std_oos_rate"].fillna(0)
    return df


# -----------------------
# 4) 시각화
# -----------------------
def draw_heatmap_store_size(df, e_code, start, end, outdir="out"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pivot = df.pivot_table(
        index="store_name",
        columns="size_display",
        values="oos_rate",
        aggfunc="mean"
    ).fillna(0)

    # 품절 비율 높은 매장이 위로 오도록 정렬
    pivot["__row_mean__"] = pivot.mean(axis=1)
    pivot = pivot.sort_values("__row_mean__", ascending=False).drop(columns="__row_mean__")

    plt.figure(figsize=(12, max(4, 0.45 * len(pivot.index))))
    ax = sns.heatmap(
        pivot,
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        cbar_kws={"label": "품절 비율(0~1)"},
        vmin=0, vmax=1
    )

    ax.set_title(f"[품절 패턴] 매장 × 사이즈 (상품 {e_code})\n기간: {start} ~ {end}", pad=14)
    ax.set_xlabel("사이즈(variant)")
    ax.set_ylabel("매장명")

    plt.tight_layout()
    outpath = outdir / f"heatmap_store_size_{e_code}.png"
    plt.savefig(outpath, dpi=180)
    plt.close()
    print(f"[SAVE] {outpath}")


def draw_scatter_products(df, start, end, outdir="out", x_cut=0.5, y_cut=None):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if y_cut is None:
        y_cut = float(df["std_oos_rate"].median())

    plt.figure(figsize=(12, 7))
    ax = sns.scatterplot(
        data=df,
        x="mean_oos_rate",
        y="std_oos_rate",
        size="variant_cnt",
        sizes=(40, 300),
        alpha=0.85
    )

    ax.set_title(f"[전체 상품] 품절 수준 vs 매장 간 편차\n기간: {start} ~ {end}", pad=12)
    ax.set_xlabel("평균 품절 비율(0~1)  → 높을수록 ‘자주 품절’")
    ax.set_ylabel("매장 간 편차(표준편차) → 높을수록 ‘매장별 차이 큼’")

    ax.axvline(x_cut, linestyle="--", linewidth=1)
    ax.axhline(y_cut, linestyle="--", linewidth=1)

    plt.tight_layout()
    outpath = outdir / "scatter_products.png"
    plt.savefig(outpath, dpi=180)
    plt.close()
    print(f"[SAVE] {outpath}")
    return x_cut, y_cut


# -----------------------
# 5) 대표 8개 자동선정 + 히트맵 일괄 생성
# -----------------------
def select_representative_8(df, x_cut=0.5, y_cut=None, n_each=4):
    """
    지금 너희 데이터는 왼쪽(평균 품절 낮음)이 비어있을 수 있어서
    '오른쪽 위(Q1) / 오른쪽 아래(Q4)' 2그룹에서 각각 4개씩 뽑는다.

    - Q1(오른쪽 위): 평균도 높고, 매장편차도 큼 -> "배분/운영 이슈형"
      정렬: mean*std(강도) 큰 순

    - Q4(오른쪽 아래): 평균은 높은데, 매장편차는 낮음 -> "공급 부족형"
      정렬: mean 높은 순 + std 낮은 순
    """
    df = df.copy()
    if y_cut is None:
        y_cut = float(df["std_oos_rate"].median())

    df["group"] = "OTHER"
    df.loc[(df["mean_oos_rate"] >= x_cut) & (df["std_oos_rate"] >= y_cut), "group"] = "Q1"
    df.loc[(df["mean_oos_rate"] >= x_cut) & (df["std_oos_rate"] <  y_cut), "group"] = "Q4"

    q1 = df[df["group"] == "Q1"].copy()
    q4 = df[df["group"] == "Q4"].copy()

    q1["score"] = q1["mean_oos_rate"] * q1["std_oos_rate"]
    q1_pick = q1.sort_values(["score", "mean_oos_rate", "std_oos_rate"], ascending=False).head(n_each)

    q4_pick = q4.sort_values(["mean_oos_rate", "std_oos_rate"], ascending=[False, True]).head(n_each)

    picked = pd.concat([q1_pick, q4_pick], axis=0)
    return picked, x_cut, y_cut


def generate_heatmaps_for_picked(engine, picked_df, start, end, outdir="out/heatmaps_8"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # 선정 목록 저장(포폴/보고서에 그대로 붙이기 좋음)
    summary_path = outdir / "picked_8_summary.csv"
    picked_df[["e_code", "group", "mean_oos_rate", "std_oos_rate", "variant_cnt", "obs_cnt"]].to_csv(
        summary_path, index=False, encoding="utf-8-sig"
    )
    print(f"[SAVE] {summary_path}")

    for _, row in picked_df.iterrows():
        e_code = row["e_code"]
        df_hm = load_store_size_heatmap_data(engine, e_code, start, end)

        # 데이터가 없으면 스킵
        if df_hm.empty:
            print(f"[SKIP] no data for {e_code}")
            continue

        # 파일명에 그룹/지표를 같이 넣어두면 정리 편함
        tag = row["group"]
        outpath = outdir / f"{tag}_heatmap_{e_code}.png"

        # draw 함수는 기본 outdir 기반 저장이라, 잠깐 outdir 바꿔 저장
        # (파일명을 커스텀하고 싶어서 여기서만 예외 처리)
        pivot = df_hm.pivot_table(
            index="store_name",
            columns="size_display",
            values="oos_rate",
            aggfunc="mean"
        ).fillna(0)

        pivot["__row_mean__"] = pivot.mean(axis=1)
        pivot = pivot.sort_values("__row_mean__", ascending=False).drop(columns="__row_mean__")

        plt.figure(figsize=(12, max(4, 0.45 * len(pivot.index))))
        ax = sns.heatmap(
            pivot,
            annot=True,
            fmt=".2f",
            linewidths=0.5,
            cbar_kws={"label": "품절 비율(0~1)"},
            vmin=0, vmax=1
        )

        ax.set_title(
            f"[{tag}] 매장×사이즈 품절 히트맵 (상품 {e_code})\n기간: {start} ~ {end}\n"
            f"평균품절={row['mean_oos_rate']:.2f}, 매장편차={row['std_oos_rate']:.2f}, variant={int(row['variant_cnt'])}",
            pad=14
        )
        ax.set_xlabel("사이즈(variant)")
        ax.set_ylabel("매장명")

        plt.tight_layout()
        plt.savefig(outpath, dpi=180)
        plt.close()
        print(f"[SAVE] {outpath}")


# -----------------------
# 6) main
# -----------------------
def main():
    setup_korean_font()

    start = os.getenv("VIZ_START", "2025-12-29")
    end = os.getenv("VIZ_END", "2026-01-03")

    # 히트맵 예시 1장(원하면 유지)
    example_e_code = os.getenv("VIZ_ECODE", "E475344-000")

    engine = get_engine()

    # (A) 산점도용 데이터 로드
    df_sc = load_product_scatter_data(engine, start, end)

    # (B) 산점도 저장 + 컷라인(세로=0.5, 가로=std 중앙값) 계산
    x_cut = float(os.getenv("SCATTER_XCUT", "0.5"))
    _, y_cut = draw_scatter_products(df_sc, start, end, outdir="out", x_cut=x_cut, y_cut=None)

    # (C) 대표 8개 자동 선정 (오른쪽 위 4 + 오른쪽 아래 4)
    picked8, x_cut, y_cut = select_representative_8(df_sc, x_cut=x_cut, y_cut=y_cut, n_each=4)
    print("\n[대표 8개 선정 결과]")
    print(picked8[["e_code", "group", "mean_oos_rate", "std_oos_rate", "variant_cnt", "obs_cnt"]])

    # (D) 대표 8개 히트맵 8장 자동 생성
    generate_heatmaps_for_picked(engine, picked8, start, end, outdir="out/heatmaps_8")

    # (옵션) 예시 1장도 별도로 저장하고 싶으면 유지
    if example_e_code:
        df_hm = load_store_size_heatmap_data(engine, example_e_code, start, end)
        if not df_hm.empty:
            draw_heatmap_store_size(df_hm, example_e_code, start, end, outdir="out")

    print("\nDONE")


if __name__ == "__main__":
    main()