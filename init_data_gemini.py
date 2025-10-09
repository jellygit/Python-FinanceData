#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FinanceDataReader를 사용하여 다수 시장의 종목 정보 및 주가 데이터를
SQLite DB에 저장합니다. (종목 정보 테이블 분리 저장)
"""

import os
import sqlite3
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from tqdm import tqdm
import argparse
import sys
import re

# --- 상수 정의 ---
DB_FILE: str = "stock_price.db"
# [수정] DB_PATH 전역 변수 제거 (main 함수에서 동적으로 생성)
DEFAULT_START_DATE: str = "2008-01-01"
MAX_WORKERS: int = 3
REQUEST_TIMEOUT: int = 5


def setup_database(db_path: str) -> None:
    """주가 데이터를 저장할 통합 테이블을 초기화합니다."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_price (
            Symbol TEXT, Date TEXT, Open REAL, High REAL, Low REAL, Close REAL,
            Volume INTEGER, Change REAL, PRIMARY KEY (Symbol, Date)
        )
        """)
        conn.commit()


def sanitize_table_name(name):
    """테이블 이름에 사용할 수 없는 문자를 '_'로 변경합니다."""
    return re.sub(r"[^A-Za-z0-9_]", "_", name)


def save_market_info_to_db(db_path: str, market_name: str, df: pd.DataFrame) -> None:
    """
    지정된 시장의 종목 목록 전체를 해당 시장 이름의 테이블에 저장합니다.
    테이블이 이미 존재하면 내용을 모두 지우고 새로 덮어씁니다.
    """
    with sqlite3.connect(db_path) as conn:
        safe_market_name = sanitize_table_name(market_name)
        # [수정] KRX 종목은 Symbol을 인덱스로 사용하지 않도록 index=False 추가
        df.to_sql(safe_market_name, conn, if_exists="replace", index=False)
    print(f"[{market_name}] 시장의 종목 정보 {len(df)}개를 DB에 저장했습니다.")


def get_last_date(db_path: str, symbol: str) -> Optional[str]:
    """DB에서 특정 종목의 가장 마지막 날짜를 조회합니다."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(Date) FROM stock_price WHERE Symbol = ?", (symbol,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None


def fetch_stock_data(
    symbol: str, start_date: str
) -> Optional[Tuple[str, pd.DataFrame]]:
    """단일 종목의 시세 데이터를 가져옵니다."""
    df = fdr.DataReader(symbol, start=start_date)
    if df.empty:
        return None
    return symbol, df


def save_price_to_db(db_path: str, symbol: str, df: pd.DataFrame) -> None:
    """주가 데이터를 'stock_price' 테이블에 저장합니다 (INSERT OR REPLACE 사용)."""
    with sqlite3.connect(db_path) as conn:
        df_to_save = df.reset_index()
        if "index" in df_to_save.columns:
            df_to_save.rename(columns={"index": "Date"}, inplace=True)
        df_to_save["Symbol"] = symbol
        if "Change" not in df_to_save.columns:
            df_to_save["Change"] = df_to_save["Close"].pct_change().fillna(0)

        required_cols = [
            "Symbol",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "Change",
        ]
        # 누락된 컬럼이 있다면 0으로 채움
        for col in required_cols:
            if col not in df_to_save.columns:
                df_to_save[col] = 0

        df_to_save = df_to_save[required_cols].copy()
        df_to_save["Date"] = pd.to_datetime(df_to_save["Date"]).dt.strftime("%Y-%m-%d")
        df_to_save.fillna(0, inplace=True)

        cursor = conn.cursor()
        data_tuples = [tuple(x) for x in df_to_save.to_numpy()]
        cursor.executemany(
            f"""
            INSERT OR REPLACE INTO stock_price ({", ".join(required_cols)}) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            data_tuples,
        )
        conn.commit()


def process_single_symbol(symbol_info: dict, db_path: str):
    """단일 종목에 대한 증분 업데이트를 처리하는 워커 함수."""
    symbol = symbol_info["Symbol"]
    last_date_str = get_last_date(db_path, symbol)
    start_date = (
        (datetime.strptime(last_date_str, "%Y-%m-%d") + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
        if last_date_str
        else DEFAULT_START_DATE
    )
    if start_date >= datetime.now().strftime("%Y-%m-%d"):
        return
    result = fetch_stock_data(symbol, start_date)
    if result:
        _, df = result
        save_price_to_db(db_path, symbol, df)


def update_symbols(market: str, db_path: str) -> None:
    """지정된 시장의 모든 종목 데이터를 가져와 DB에 저장합니다."""
    print(f"\n[{market}] 시장 정보 수집을 시작합니다...")
    symbols_df: pd.DataFrame = fdr.StockListing(market)

    if "Code" in symbols_df.columns and "Symbol" not in symbols_df.columns:
        symbols_df.rename(columns={"Code": "Symbol"}, inplace=True)

    if symbols_df.empty:
        print(f"'{market}' 시장에서 종목 목록을 가져올 수 없습니다.")
        return

    save_market_info_to_db(db_path, market, symbols_df)


def update_prices(market: str, db_path: str, start_year: Optional[int] = None) -> None:
    """지정된 거래소의 종목 시세를 'stock_prices' 테이블에 추가합니다."""
    print(f"[{market}] 종목별 시세 데이터 업데이트를 시작합니다.")
    table_name = sanitize_table_name(market)

    try:
        with sqlite3.connect(db_path) as con:
            symbols_df = pd.read_sql_query(
                f'SELECT Symbol, Name FROM "{table_name}"', con
            )
            symbol_list = symbols_df.to_dict("records")
    except Exception as e:
        print(
            f"[{market}] '{table_name}' 테이블에서 종목 목록을 불러오는 중 오류 발생: {e}",
            file=sys.stderr,
        )
        return

    global DEFAULT_START_DATE
    if start_year:
        DEFAULT_START_DATE = f"{start_year}-01-01"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_symbol, symbol_info, db_path): symbol_info
            for symbol_info in symbol_list
        }
        for future in tqdm(
            as_completed(futures),
            total=len(symbol_list),
            desc=f"Processing Prices in {market}",
        ):
            symbol_info = futures[future]
            try:
                future.result(timeout=REQUEST_TIMEOUT)
            except TimeoutError:
                tqdm.write(
                    f"오류: 종목 '{symbol_info['Symbol']}' 데이터 요청 시간 초과({REQUEST_TIMEOUT}초). 건너뜁니다."
                )
            except Exception as e:
                tqdm.write(
                    f"오류: 종목 '{symbol_info['Symbol']}' 처리 중 문제 발생 - {type(e).__name__}: {e}"
                )

    print(f"[{market}] 시장 시세 데이터 수집 완료.")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(
        description="주식 종목 및 시세 데이터를 초기화하고 업데이트합니다.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "markets",
        nargs="+",
        help="처리할 거래소 목록 (공백으로 구분)\n(예: KRX NASDAQ ETF/US)",
    )
    parser.add_argument(
        "--update-symbols",
        action="store_true",
        help="전체 종목 목록을 새로 받아와 거래소별 테이블에 덮어씁니다.",
    )
    parser.add_argument(
        "--update-prices",
        action="store_true",
        help="종목별 시세 데이터를 'stock_prices' 테이블에 추가합니다 (중복 제외).",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help=f"시세 데이터를 받아올 시작 연도 (기본값: {DEFAULT_START_DATE.split('-')[0]})",
    )
    args = parser.parse_args()

    if not args.update_symbols and not args.update_prices:
        parser.print_help()
        sys.exit(
            "\n오류: --update-symbols 와 --update-prices 중 하나 이상의 작업을 선택해야 합니다."
        )

    # [수정] 스크립트 위치를 기준으로 정확한 DB 파일 경로 생성
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILE)
    setup_database(db_path)

    for market in args.markets:
        print(f"\n===== '{market}' 거래소 작업 시작 =====")
        if args.update_symbols:
            # [수정] 정확한 db_path 전달
            update_symbols(market, db_path)

        if args.update_prices:
            # [수정] 정확한 db_path 전달
            update_prices(market, db_path, args.start_year)
        print(f"===== '{market}' 거래소 작업 완료 =====\n")


if __name__ == "__main__":
    main()
