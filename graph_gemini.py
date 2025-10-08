#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
'stock_monthly_summary_with_roi.csv' 데이터를 사용하여
종목별 투자 성과 그래프를 병렬로 생성합니다. (종목명 포함)

- Y축(좌): 투입금, 평가금액 (로그 스케일)
- Y축(우): 손익률(%)
- X축: 날짜 (시계열)
- 멀티프로세싱을 사용하여 생성 속도를 개선합니다.
"""

import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.font_manager as fm
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from typing import Tuple, Dict

# --- 설정 ---
INPUT_CSV = "stock_monthly_summary_with_roi.csv"
DB_FILE = "stock_price.db"
OUTPUT_DIR = "charts"
FONT_PATH = "./fonts/goorm-sans-code.ttf"
fm.fontManager.addfont(FONT_PATH)
FONT_NAME = fm.FontProperties(fname=FONT_PATH).get_name()
plt.rc("font", family=FONT_NAME)
plt.rc("axes", unicode_minus=False)  # 축 마이너스 부호 깨짐 방지


def get_symbol_to_name_map(db_path: str) -> Dict[str, str]:
    """
    DB에 연결하여 모든 테이블에서 {종목코드: 종목명} 맵을 생성합니다.
    """
    name_map = {}
    if not os.path.exists(db_path):
        print(f"경고: DB 파일 '{db_path}'를 찾을 수 없어 종목명이 표시되지 않습니다.")
        return name_map

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
            table_names = [row[0] for row in cursor.fetchall()]

            for table_name in table_names:
                try:
                    cursor.execute(f'SELECT Symbol, Name FROM "{table_name}"')
                    for symbol, name in cursor.fetchall():
                        name_map[symbol] = name
                except sqlite3.OperationalError:
                    continue  # 'Symbol' 또는 'Name' 컬럼이 없는 테이블은 건너뜀
    except sqlite3.Error as e:
        print(f"DB 오류 발생: {e}")

    return name_map


def create_chart(args: Tuple[str, pd.DataFrame, Dict[str, str]]) -> str:
    """
    단일 종목의 데이터프레임을 받아 그래프를 생성하고 파일로 저장합니다.

    Args:
        args (Tuple[str, pd.DataFrame, Dict[str, str]]):
            (종목 코드, 해당 종목의 데이터프레임, 전체 종목명 맵)

    Returns:
        str: 성공적으로 생성된 종목 코드.
    """
    symbol, df, name_map = args
    stock_name = name_map.get(symbol, "")  # 맵에서 종목명 조회, 없으면 빈 문자열

    # 1. Matplotlib Figure 및 Axes 설정
    fig, ax1 = plt.subplots(figsize=(15, 8))

    # 2. 왼쪽 Y축 (ax1): 투입금, 평가금액 (로그 스케일)
    ax1.plot(
        df["Date"],
        df["TotalInvestment"],
        color="gray",
        linestyle="--",
        label="Total Investment (L)",
    )
    ax1.plot(
        df["Date"],
        df["PortfolioValue"],
        color="blue",
        linewidth=2,
        label="Portfolio Value (L)",
    )

    ax1.set_yscale("log")
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Amount (Log Scale)", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.yaxis.set_major_formatter(
        mticker.FuncFormatter(
            lambda x, p: f"{x / 1000000:.1f}M" if x >= 1000000 else f"{x / 1000:.0f}K"
        )
    )
    ax1.grid(True, which="both", ls="--", linewidth=0.5)

    # 3. 오른쪽 Y축 (ax2): 손익률(ROI)
    ax2 = ax1.twinx()
    ax2.plot(
        df["Date"], df["ROI_Percent"], color="green", alpha=0.8, label="ROI (%) (R)"
    )
    ax2.set_ylabel("ROI (%)", color="green")
    ax2.tick_params(axis="y", labelcolor="green")
    ax2.axhline(0, color="red", linestyle=":", linewidth=1)

    # 4. 그래프 종합 설정 (종목명 포함)
    chart_title = f"Investment Backtest: {symbol}"
    if stock_name:
        chart_title += f" ({stock_name})"
    fig.suptitle(chart_title, fontsize=16)
    fig.autofmt_xdate()

    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc="upper left")

    fig.tight_layout(rect=[0, 0, 1, 0.96])

    # 5. 파일로 저장
    filepath = os.path.join(OUTPUT_DIR, f"{symbol}.webp")
    plt.savefig(filepath, dpi=150)
    plt.close(fig)

    return symbol


def main():
    """메인 실행 함수"""
    if not os.path.exists(INPUT_CSV):
        print(f"오류: 입력 파일 '{INPUT_CSV}'을 찾을 수 없습니다.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("DB에서 종목명 정보를 가져오는 중...")
    symbol_name_map = get_symbol_to_name_map(DB_FILE)

    print(f"'{INPUT_CSV}' 파일에서 데이터를 읽는 중...")
    df = pd.read_csv(INPUT_CSV)
    df["Date"] = pd.to_datetime(df["Date"])

    # 멀티프로세싱을 위해 (symbol, group_df, name_map) 튜플의 리스트 생성
    grouped = df.groupby("Symbol")
    tasks = [(symbol, group_df, symbol_name_map) for symbol, group_df in grouped]

    print(f"{len(tasks)}개 종목에 대한 그래프 생성을 시작합니다...")
    max_workers = os.cpu_count()

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        list(
            tqdm(
                executor.map(create_chart, tasks),
                total=len(tasks),
                desc="그래프 생성 중",
            )
        )

    print("\n" + "=" * 50)
    print(f"✅ 모든 그래프 생성이 완료되었습니다.")
    print(f"   - 저장된 폴더: '{OUTPUT_DIR}'")
    print("=" * 50)


if __name__ == "__main__":
    main()
