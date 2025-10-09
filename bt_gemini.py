#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
'stock_price.db'의 통합 테이블을 사용하여 전체 시장 종목에 대한
장기 적립식 투자 백테스트를 실행합니다. (멀티프로세싱, 손익률 추가)

- DB에 있는 모든 종목에 대해 개별 백테스트 실행
- 개별 종목의 월별 평가액, 보유 주식 수, 누적 투자금, 손익률을 추적하여 단일 CSV 파일로 저장
"""

import os
import sqlite3
import pandas as pd
import holidays
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- 백테스트 설정 ---
DB_FILE: str = "stock_price.db"
# DB_FILE: str = "db/finance.db"
# 투자 기간 (년)
YEARS_TO_TEST: int = 10
# 월별 투자 금액 (종목당)
MONTHLY_INVESTMENT_PER_STOCK: float = 100_000
# 매수 규칙: 'first' (월초), 'last' (월말)
PURCHASE_RULE: str = "first"
# 국가 코드 (공휴일 계산용): 'KR' (한국), 'US' (미국)
COUNTRY_CODE: str = "KR"


def get_all_symbols(db_path: str, *markets: str) -> List[str]:
    """
    지정된 SQLite DB에 연결하여, 명시된 시장(테이블)들의 'Symbol' 컬럼 데이터를
    중복 없이 가져옵니다.

    만약 markets 인자가 비어있으면, DB 내의 모든 테이블을 대상으로 합니다.
    """
    all_symbols_set = set()
    tables_to_scan = list(markets)

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            if not tables_to_scan:
                print(
                    "INFO: 조회할 시장이 지정되지 않아 DB의 모든 테이블을 검색합니다."
                )
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
                tables_to_scan = [row[0] for row in cursor.fetchall()]

            for table_name in tables_to_scan:
                try:
                    cursor.execute(f'SELECT Symbol FROM "{table_name}"')
                    symbols_in_table = [row[0] for row in cursor.fetchall()]
                    all_symbols_set.update(symbols_in_table)
                except sqlite3.OperationalError:
                    print(
                        f"경고: '{table_name}' 테이블을 찾을 수 없거나 'Symbol' 컬럼이 없습니다."
                    )

        return list(all_symbols_set)

    except sqlite3.Error as e:
        print(f"데이터베이스 오류가 발생했습니다: {e}")
        return []


def get_stock_data(
    symbol: str, db_path: str, start_date: str, end_date: str
) -> Optional[pd.DataFrame]:
    """SQLite DB에서 특정 종목의 시세 데이터를 DataFrame으로 불러옵니다."""
    with sqlite3.connect(db_path) as conn:
        query = f"""
        SELECT Date, Close FROM stock_price 
        WHERE Symbol = ? AND Date BETWEEN ? AND ?
        ORDER BY Date ASC
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_date, end_date))

    if df.empty:
        return None

    df["Date"] = pd.to_datetime(df["Date"])
    df.set_index("Date", inplace=True)
    return df


def get_monthly_purchase_dates(
    start_date: datetime, end_date: datetime, rule: str, country_code: str
) -> List[datetime]:
    """주어진 규칙에 따라 월별 매수 날짜(영업일) 리스트를 생성합니다."""
    country_holidays = holidays.country_holidays(country_code)
    purchase_dates: List[datetime] = []
    current_date = start_date

    while current_date <= end_date:
        if rule == "first":
            target_date = current_date.replace(day=1)
        elif rule == "last":
            next_month = current_date.replace(day=28) + relativedelta(days=4)
            target_date = next_month - relativedelta(days=next_month.day)
        else:
            target_date = current_date.replace(day=1)

        while target_date.weekday() >= 5 or target_date in country_holidays:
            target_date += relativedelta(days=1)

        purchase_dates.append(target_date)
        current_date += relativedelta(months=1)

    return purchase_dates


def run_single_stock_backtest(
    prices_df: pd.DataFrame, purchase_dates: List[datetime], monthly_investment: float
) -> Optional[Dict[str, Any]]:
    """단일 종목에 대한 백테스트를 실행하고, 시계열 데이터를 포함한 결과를 반환합니다."""
    shares_ts = pd.Series(0.0, index=prices_df.index, dtype=float)

    for date in purchase_dates:
        try:
            trade_date = prices_df.index.asof(date)
            price = prices_df.loc[trade_date, "Close"]
            if pd.isna(price) or price <= 0:
                continue
            shares_bought = monthly_investment / price
            shares_ts.loc[trade_date:] += shares_bought
        except (KeyError, IndexError):
            continue

    if shares_ts.sum() == 0:
        return None

    portfolio_ts = prices_df["Close"] * shares_ts
    total_investment = len(purchase_dates) * monthly_investment
    final_value = portfolio_ts.iloc[-1]

    return {
        "total_investment": total_investment,
        "final_value": final_value,
        "roi": (final_value - total_investment) / total_investment * 100
        if total_investment > 0
        else 0,
        "portfolio_ts": portfolio_ts.fillna(0),
        "shares_ts": shares_ts.fillna(0),
    }


def process_single_symbol(symbol: str) -> Optional[pd.DataFrame]:
    """단일 종목에 대한 전체 처리(데이터 로드, 백테스트, 결과 가공)를 수행하는 워커 함수."""
    end_date = datetime.now()
    start_date = end_date - relativedelta(years=YEARS_TO_TEST)

    prices = get_stock_data(
        symbol, DB_FILE, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
    )

    if prices is None or len(prices) < 20:
        return None

    dates_to_buy = get_monthly_purchase_dates(
        prices.index.min(), prices.index.max(), PURCHASE_RULE, COUNTRY_CODE
    )
    result = run_single_stock_backtest(
        prices, dates_to_buy, MONTHLY_INVESTMENT_PER_STOCK
    )

    if result:
        # 월별 데이터로 리샘플링
        monthly_value = result["portfolio_ts"].resample("ME").last()
        monthly_shares = result["shares_ts"].resample("ME").last()

        # 월별 누적 투자 원금 계산
        month_counts = pd.Series(
            range(1, len(monthly_value) + 1), index=monthly_value.index
        )
        monthly_investment_ts = month_counts * MONTHLY_INVESTMENT_PER_STOCK

        # 월별 손익률(ROI) 계산
        roi_ts = (monthly_value - monthly_investment_ts) / monthly_investment_ts * 100
        roi_ts = roi_ts.fillna(0).replace([float("inf"), -float("inf")], 0)

        # 월별 데이터를 DataFrame으로 생성
        monthly_df = pd.DataFrame(
            {
                "TotalInvestment": monthly_investment_ts,
                "PortfolioValue": monthly_value,
                "TotalShares": monthly_shares,
                "ROI_Percent": roi_ts,
            }
        )
        monthly_df["Symbol"] = symbol

        monthly_df.attrs["summary"] = (
            f"[결과] 종목: {symbol} | 총투자: {result['total_investment']:,.0f}원 | "
            f"최종평가액: {result['final_value']:,.0f}원 | 수익률: {result['roi']:.2f}%"
        )
        return monthly_df
    return None


def main():
    """메인 실행 함수"""
    # 백테스트를 원하는 시장을 직접 지정
    # target_markets = ["ETF/US"]
    target_markets = ["KRX", "ETF_US", "NYSE", "NASDAQ"]
    # target_markets = ["ETF/KR", "KRX"]
    # target_markets = ["NASDAQ", "NYSE", "KRX"]
    print(f"DB에서 {target_markets} 시장의 종목 코드를 가져옵니다...")
    all_symbols = get_all_symbols(DB_FILE, *target_markets)

    if not all_symbols:
        print("오류: 지정된 시장에서 종목을 찾을 수 없습니다.")
        return

    all_stocks_monthly_data = []
    max_workers = os.cpu_count()
    print(
        f"{len(all_symbols)}개 종목에 대한 백테스트를 시작합니다 (최대 {max_workers}개 프로세스 사용)..."
    )

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_single_symbol, symbol): symbol
            for symbol in all_symbols
        }

        for future in tqdm(
            as_completed(futures), total=len(all_symbols), desc="백테스팅 진행"
        ):
            result_df = future.result()
            if result_df is not None:
                all_stocks_monthly_data.append(result_df)
                tqdm.write(result_df.attrs["summary"])

    if all_stocks_monthly_data:
        final_summary_df = pd.concat(all_stocks_monthly_data)
        final_summary_df.reset_index(inplace=True)
        # 컬럼 순서 재정렬
        final_summary_df = final_summary_df[
            [
                "Symbol",
                "Date",
                "TotalInvestment",
                "PortfolioValue",
                "TotalShares",
                "ROI_Percent",
            ]
        ]

        output_filename = "stock_monthly_summary_with_roi.csv"
        final_summary_df.to_csv(output_filename, index=False, float_format="%.2f")

        print("\n" + "=" * 60)
        print(
            "💰 종목별 월별 투자 결과(손익률 포함)가 CSV 파일로 성공적으로 저장되었습니다."
        )
        print(f"   - 파일명: {output_filename}")
        print("=" * 60)
    else:
        print("\n백테스트를 완료할 수 있는 데이터가 부족합니다.")


if __name__ == "__main__":
    main()
