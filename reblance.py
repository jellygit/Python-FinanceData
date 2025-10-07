#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
지정된 투자금과 포트폴리오(종목 및 비율)에 대해,
과거 특정 시점부터 주기적으로 자산 평가액을 추적하는 시뮬레이터.
"""

import sqlite3
import argparse
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple


def parse_args():
    """사용자 입력을 파싱하고 유효성을 검사합니다."""
    parser = argparse.ArgumentParser(
        description="주기적 포트폴리오 가치 평가 시뮬레이터"
    )
    parser.add_argument(
        "investment", type=float, help="초기 총 투자 금액 (예: 1000000)"
    )
    parser.add_argument(
        "portfolio", nargs="+", help="종목과 비율의 쌍 (예: AAPL 0.5 NVDA 0.5)"
    )
    parser.add_argument(
        "--start-date", required=True, help="평가 시작일 (YYYY-MM-DD 형식)"
    )
    parser.add_argument(
        "--interval", required=True, help="평가 간격 (예: '1M' (1개월), '3M' (3개월))"
    )
    parser.add_argument(
        "--db-path", default="stock_price.db", help="SQLite DB 파일 경로"
    )

    args = parser.parse_args()

    # 포트폴리오 인자 파싱 (티커, 비율)
    if len(args.portfolio) % 2 != 0:
        raise ValueError("종목과 비율은 반드시 쌍으로 입력해야 합니다.")

    tickers = args.portfolio[::2]
    ratios = [float(r) for r in args.portfolio[1::2]]

    if not np.isclose(sum(ratios), 1.0):
        print(f"경고: 비율의 합이 {sum(ratios):.2f}입니다. 1.0이 되도록 정규화합니다.")
        total_ratio = sum(ratios)
        ratios = [r / total_ratio for r in ratios]

    return (
        args.investment,
        dict(zip(tickers, ratios)),
        args.start_date,
        args.interval,
        args.db_path,
    )


def load_price_data(db_path: str, tickers: List[str], start_date: str) -> pd.DataFrame:
    """DB에서 필요한 기간의 모든 종목 가격을 불러와 가공합니다."""
    try:
        with sqlite3.connect(db_path) as con:
            ticker_list_str = ", ".join([f"'{t}'" for t in tickers])
            query = f"""
            SELECT Date as date, Symbol as ticker, Close as close_price
            FROM stock_price
            WHERE Symbol IN ({ticker_list_str}) AND Date >= '{start_date}'
            ORDER BY Date ASC;
            """
            df = pd.read_sql_query(query, con, parse_dates=["date"])

        # 데이터를 피벗하여 날짜는 인덱스, 티커는 컬럼으로 만듭니다.
        price_df = df.pivot(index="date", columns="ticker", values="close_price")
        # 누락된 데이터를 이전 거래일 데이터로 채웁니다 (휴일 등)
        price_df.ffill(inplace=True)
        return price_df
    except Exception as e:
        print(f"데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()


def get_next_trading_day(date: datetime, available_dates: pd.DatetimeIndex) -> datetime:
    """주어진 날짜 혹은 그 이후의 가장 가까운 첫 영업일을 찾습니다."""
    while date not in available_dates:
        date += timedelta(days=1)
        if date > available_dates.max():  # 데이터 범위를 넘어서면 마지막 날짜 반환
            return available_dates.max()
    return date


def simulate_initial_purchase(
    investment: float,
    ratios: Dict[str, float],
    purchase_date: datetime,
    price_data: pd.DataFrame,
) -> [Tuple, float]:
    """초기 투자일에 각 종목을 매수하고 보유 주식 수와 남은 현금을 계산합니다."""
    holdings = {}
    cash = investment

    print("\n--- 초기 투자 시뮬레이션 ---")
    print(f"매수 기준일: {purchase_date.strftime('%Y-%m-%d')}")

    for ticker, ratio in ratios.items():
        amount_to_invest = investment * ratio
        price = price_data.loc[purchase_date, ticker]

        if pd.isna(price) or price <= 0:
            print(f"경고: {ticker}의 가격 정보가 없어 매수할 수 없습니다.")
            continue

        shares_to_buy = int(amount_to_invest // price)
        cost = shares_to_buy * price

        if shares_to_buy > 0:
            holdings[ticker] = shares_to_buy
            cash -= cost
            print(
                f"- {ticker}: {shares_to_buy}주 매수 (주당 {price:,.2f}, 총 {cost:,.2f})"
            )
        else:
            print(
                f"- {ticker}: 투자금({amount_to_invest:,.2f})이 주가({price:,.2f})보다 작아 매수 불가"
            )

    print(f"초기 매수 후 남은 현금: {cash:,.2f}")
    return holdings, cash


def main():
    """메인 실행 로직"""
    try:
        investment, ratios, start_date_str, interval, db_path = parse_args()
    except ValueError as e:
        print(f"입력 오류: {e}")
        return

    tickers = list(ratios.keys())

    # 1. 데이터 로드
    print("데이터를 로딩합니다...")
    price_data = load_price_data(db_path, tickers, start_date_str)
    if price_data.empty:
        print(
            "백테스트에 필요한 데이터를 찾을 수 없습니다. 종목 코드나 날짜를 확인해주세요."
        )
        return

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    # 2. 초기 투자 실행
    initial_purchase_date = get_next_trading_day(start_date, price_data.index)
    holdings, cash = simulate_initial_purchase(
        investment, ratios, initial_purchase_date, price_data
    )

    # 3. 평가일 리스트 생성
    evaluation_dates_theory = pd.date_range(
        start=start_date, end=datetime.now(), freq=interval
    )
    evaluation_dates_actual = sorted(
        list(
            set(
                [
                    get_next_trading_day(d, price_data.index)
                    for d in evaluation_dates_theory
                ]
            )
        )
    )

    # 4. 주기적 가치 평가 (⭐ 수정: 상세 정보 수집)
    results = []
    for eval_date in evaluation_dates_actual:
        stock_value = 0
        row_data = {}  # 현재 행(날짜)의 모든 데이터를 담을 딕셔너리

        # 각 종목의 가치를 계산하고 상세 정보 기록
        for ticker, shares in holdings.items():
            price = price_data.loc[eval_date, ticker]
            stock_value += shares * price
            row_data[f"{ticker}_Price"] = price
            row_data[f"{ticker}_Shares"] = shares

        total_value = stock_value + cash

        # 최종 행 데이터 구성
        row_data["Date"] = eval_date.strftime("%Y-%m-%d")
        row_data["Portfolio Value"] = total_value
        results.append(row_data)

    # 5. 결과 출력 (⭐ 수정: 상세 DataFrame 생성 및 포맷팅)
    print("\n--- 포트폴리오 가치 평가 결과 ---")
    if not results:
        print("평가 결과가 없습니다.")
        return

    result_df = pd.DataFrame(results)

    # 컬럼 순서 재정렬
    column_order = []
    for ticker in tickers:
        column_order.append(f"{ticker}_Price")
        column_order.append(f"{ticker}_Shares")
    result_df = result_df[column_order]

    # 컬럼 이름 변경
    new_column_names = {"Portfolio Value": "Portfolio Value"}
    for ticker in tickers:
        new_column_names[f"{ticker}_Price"] = f"{ticker} 1주 가격"
        new_column_names[f"{ticker}_Shares"] = f"{ticker} 주식보유수"
    result_df.rename(columns=new_column_names, inplace=True)

    # 숫자 포맷팅
    result_df["Portfolio Value"] = result_df["Portfolio Value"].apply(
        lambda x: f"{x:,.2f}"
    )
    for ticker in tickers:
        result_df[f"{ticker} 1주 가격"] = result_df[f"{ticker} 1주 가격"].apply(
            lambda x: f"{x:,.2f}"
        )
        result_df[f"{ticker} 주식보유수"] = result_df[f"{ticker} 주식보유수"].apply(
            lambda x: f"{x:,.0f}"
        )

    print(result_df.to_string(index=False))


if __name__ == "__main__":
    main()
