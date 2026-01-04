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
    macOS: AppleGothic 우선
    (추가로 NanumGothic 설치돼 있으면 그걸로도 됨)
    """
    candidates = [
        "AppleGothic",
        "NanumGothic",
        "Noto Sans CJK KR",
        "Noto Sans KR",
        "Malgun Gothic",
    ]

    installed = {f.name for f in fm.fontManager.ttflist}
    chosen = next((f for f in candidates if f in installed), None)

    if not chosen:
        print("[FONT] 한글 폰트를 못 찾음. brew로 font-nanum-gothic 설치 권장")
        chosen = "sans-serif"

    # matplotlib 전역 폰트 지정 (중요)
    mpl.rcParams["font.family"] = chosen
    mpl.rcParams["font.sans-serif"] = [chosen]
    mpl.rcParams["axes.unicode_minus"] = False

    # seaborn에도 폰트 강제 (중요)
    sns.set_theme(style="whitegrid", font=chosen)

    print(f"[FONT] chosen={chosen}")

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
            f"(.env에 MYSQL_HOST, MYSQL_USER, MYSQL_DB 설정 필요)"
        )

    # 비번이 빈 문자열이면 "user:@host" 형태가 되는데 pymysql은 OK
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)

# -----------------------
# 3) 데이터 로딩 (뷰를 그대로 사용)
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
    df = pd.read_sql(q, engine, params={"e_code": e_code, "start": start, "end": end})
    return df

def load_product_scatter_data(engine, start, end):
    """
    제품별로:
    - 평균 품절 비율(0~1)
    - 매장 간 편차(표준편차)
    - 변형(사이즈/색 등) 개수
    """
    q = text("""
        SELECT
          e_code,
          AVG(oos_rate) AS mean_oos_rate,
          STDDEV_POP(oos_rate) AS std_oos_rate,
          COUNT(DISTINCT l2_id) AS variant_cnt
        FROM v_agg_store_variant_daily
        WHERE dt >= :start
          AND dt <  :end
        GROUP BY e_code
    """)
    df = pd.read_sql(q, engine, params={"start": start, "end": end})
    return df

# -----------------------
# 4) 시각화
# -----------------------
def draw_heatmap_store_size(df, e_code, start, end, outdir="out"):
    """
    설명(보고서용):
    - 행: 매장
    - 열: 사이즈(또는 변형)
    - 값: '품절 비율' = 관측된 시점 중 품절(OUT_OF_STOCK)인 비율
      예) 1.00이면 관측 기간 내내 품절, 0.00이면 계속 재고 있음
    """
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pivot = df.pivot_table(
        index="store_name",
        columns="size_display",
        values="oos_rate",
        aggfunc="mean"
    ).fillna(0)

    # 보기 좋게 정렬: 품절 비율 높은 매장이 위로
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

    ax.set_title(f"[품절 패턴] 매장 × 사이즈 (상품 {e_code})\n기간: {start} ~ {end}", pad=16)
    ax.set_xlabel("사이즈(variant)")
    ax.set_ylabel("매장명")

    plt.tight_layout()
    outpath = outdir / f"heatmap_store_size_{e_code}.png"
    plt.savefig(outpath, dpi=180)
    plt.close()
    print(f"[SAVE] {outpath}")

def draw_scatter_products(df, start, end, outdir="out"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # NaN 처리(표준편차가 계산 안 되는 경우 등)
    df = df.copy()
    df["std_oos_rate"] = df["std_oos_rate"].fillna(0)

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

    # 읽기 쉬운 가이드 라인(선택)
    ax.axvline(0.5, linestyle="--", linewidth=1)
    ax.axhline(df["std_oos_rate"].median(), linestyle="--", linewidth=1)

    plt.tight_layout()
    outpath = outdir / "scatter_products.png"
    plt.savefig(outpath, dpi=180)
    plt.close()
    print(f"[SAVE] {outpath}")

# -----------------------
# 5) main
# -----------------------
def main():
    setup_korean_font()

    # 여기 기간/상품은 네가 쓰던 값으로 기본 세팅
    start = os.getenv("VIZ_START", "2025-12-29")
    end = os.getenv("VIZ_END", "2026-01-03")
    example_e_code = os.getenv("VIZ_ECODE", "E475344-000")

    engine = get_engine()

    # 1) 특정 상품 히트맵
    df_hm = load_store_size_heatmap_data(engine, example_e_code, start, end)
    draw_heatmap_store_size(df_hm, example_e_code, start, end)

    # 2) 전체 상품 산점도
    df_sc = load_product_scatter_data(engine, start, end)
    draw_scatter_products(df_sc, start, end)

    print("DONE")

if __name__ == "__main__":
    main()