#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
import seaborn as sns

load_dotenv()


def setup_korean_font():
    """
    한글 깨짐(□) 해결: matplotlib + seaborn 모두에 '한글 폰트'를 강제 적용
    """
    candidates = [
        "AppleGothic",
        "Apple SD Gothic Neo",
        "NanumGothic",
        "Noto Sans CJK KR",
        "Noto Sans KR",
        "Malgun Gothic",
        "맑은 고딕",
    ]

    installed = {f.name for f in fm.fontManager.ttflist}
    chosen = next((name for name in candidates if name in installed), None)

    if not chosen:
        print("[FONT] 한글 폰트를 못 찾음. (AppleGothic/Nanum/Noto 설치 확인)")
        chosen = "sans-serif"

    # ✅ matplotlib 전역 폰트 강제
    mpl.rcParams["font.family"] = chosen
    mpl.rcParams["font.sans-serif"] = [chosen]  # fallback도 한글 폰트로
    mpl.rcParams["axes.unicode_minus"] = False

    # ✅ seaborn에도 폰트 명시(이게 빠지면 Arial로 돌아가는 경우가 있음)
    sns.set_theme(style="whitegrid", font=chosen)

    print(f"[FONT] using: {chosen}")


def draw_top10_problem_items_by_store(df_top10, start, end, outdir="out/oos_persistence"):
    """
    df_top10 기대 컬럼:
      - store_name
      - e_code
      - max_full_oos_streak_days
      - total_full_oos_days
      - max_streak_start_dt
      - max_streak_end_dt
    """
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    def short_store(s: str, n: int = 14) -> str:
        s = str(s)
        return s if len(s) <= n else s[:n] + "…"

    plot_df = df_top10.copy()

    # y축 라벨(매장명 + 품번)
    plot_df["y_key"] = plot_df.apply(
        lambda r: f"{short_store(r['store_name'])} | {r['e_code']}",
        axis=1
    )

    # 보기: streak 작은 게 위, 큰 게 아래로(가독성)
    plot_df = plot_df.sort_values(
        ["max_full_oos_streak_days", "total_full_oos_days"],
        ascending=[True, True]
    )

    plt.figure(figsize=(14, 7))
    ax = sns.barplot(
        data=plot_df,
        y="y_key",
        x="max_full_oos_streak_days",
        orient="h"
    )

    ax.set_title(f"매장별 ‘연속 하루종일 품절’ TOP 10 문제 상품\n기간: {start} ~ {end}", pad=12)
    ax.set_xlabel("최장 연속 ‘하루종일 품절’ 일수(일)")
    ax.set_ylabel("매장 | 품번")

    # 막대 끝에: total, 기간 같이 표기
    for i, row in enumerate(plot_df.itertuples(index=False)):
        # pandas itertuples 컬럼 접근: row.max_full_oos_streak_days 형태
        x = int(row.max_full_oos_streak_days)
        total = int(row.total_full_oos_days)
        sdt = getattr(row, "max_streak_start_dt", "")
        edt = getattr(row, "max_streak_end_dt", "")
        label = f"total={total}  ({sdt}~{edt})" if sdt != "" else f"total={total}"

        ax.text(x + 0.05, i, label, va="center", fontsize=10)

    plt.tight_layout()
    outpath = outdir / "bar_top10_store_problem_items.png"
    plt.savefig(outpath, dpi=180)
    plt.close()
    print(f"[SAVE] {outpath}")


def main():
    setup_korean_font()  # ✅ 폰트는 제일 먼저

    # 입력 CSV (너가 생성한 persistence summary)
    in_path = Path("out/oos_persistence/summary_store_product_maxstreak.csv")
    if not in_path.exists():
        raise RuntimeError(
            f"파일이 없습니다: {in_path}\n"
            f"먼저 persistence 코드부터 실행해서 summary csv를 생성해줘."
        )

    df = pd.read_csv(in_path)

    # 컬럼명이 다를 수 있으니 방어적으로 체크
    required = [
        "store_name",
        "e_code",
        "max_full_oos_streak_days",
        "total_full_oos_days",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(
            f"CSV에 필요한 컬럼이 없습니다: {missing}\n"
            f"현재 컬럼: {list(df.columns)}"
        )

    # 결측/타입 정리
    df["max_full_oos_streak_days"] = pd.to_numeric(
        df["max_full_oos_streak_days"], errors="coerce"
    ).fillna(0).astype(int)

    df["total_full_oos_days"] = pd.to_numeric(
        df["total_full_oos_days"], errors="coerce"
    ).fillna(0).astype(int)

    # TOP 10: "최대 연속 품절일" 우선, 동률이면 "전체 하루종일 품절일"로 정렬
    top10 = (
        df.sort_values(["max_full_oos_streak_days", "total_full_oos_days"], ascending=False)
          .head(10)
          .copy()
    )

    outdir = Path("out/oos_persistence")
    outdir.mkdir(parents=True, exist_ok=True)

    # 보고서 붙이기 좋은 TOP10 테이블 저장
    out_csv = outdir / "top10_store_product_full_oos_streak.csv"
    top10.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"[SAVE] {out_csv}")

    # 날짜 컬럼이 없을 수도 있으니 있으면 문자열로 정리
    for c in ["max_streak_start_dt", "max_streak_end_dt"]:
        if c in top10.columns:
            top10[c] = top10[c].astype(str)

    # 그래프 저장
    start = os.getenv("VIZ_START", "")
    end = os.getenv("VIZ_END", "")
    if not start:
        start = str(top10["max_streak_start_dt"].min()) if "max_streak_start_dt" in top10.columns else ""
    if not end:
        end = str(top10["max_streak_end_dt"].max()) if "max_streak_end_dt" in top10.columns else ""

    draw_top10_problem_items_by_store(top10, start, end, outdir=str(outdir))

    print("\n[TOP10 미리보기]")
    cols = ["store_name", "e_code", "max_full_oos_streak_days", "total_full_oos_days"]
    if "max_streak_start_dt" in top10.columns:
        cols += ["max_streak_start_dt"]
    if "max_streak_end_dt" in top10.columns:
        cols += ["max_streak_end_dt"]
    print(top10[cols].to_string(index=False))


if __name__ == "__main__":
    main()