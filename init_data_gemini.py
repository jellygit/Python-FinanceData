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

# --- 상수 정의 ---
DB_FILE: str = "stock_price.db"
# MARKETS: List[str] = ["NASDAQ", "NYSE"]
# MARKETS: List[str] = ["ETF/KR"]
MARKETS: List[str] = ["ETF/US"]
# MARKETS: List[str] = ["KRX"]
DEFAULT_START_DATE: str = "2008-01-01"
MAX_WORKERS: int = 3
REQUEST_TIMEOUT: int = 3


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


def save_market_info_to_db(db_path: str, market_name: str, df: pd.DataFrame) -> None:
    """
    지정된 시장의 종목 목록 전체를 해당 시장 이름의 테이블에 저장합니다.
    테이블이 이미 존재하면 내용을 모두 지우고 새로 덮어씁니다.
    """
    with sqlite3.connect(db_path) as conn:
        # 테이블 이름에 특수문자가 있을 경우를 대비해 큰따옴표로 감싸줌
        safe_market_name = f'"{market_name}"'
        df.to_sql(safe_market_name, conn, if_exists="replace", index=True)
    print(f"[{market_name}] 시장의 종목 정보 {len(df)}개를 DB에 저장했습니다.")


def get_last_date(db_path: str, symbol: str) -> Optional[str]:
    """DB에서 특정 종목의 가장 마지막 날짜를 조회합니다."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(Date) FROM stock_price WHERE Symbol = ?", (symbol,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else None


def fetch_stock_data(
    symbol: str, start_date: str, market: str
) -> Optional[Tuple[str, pd.DataFrame]]:
    """단일 종목의 시세 데이터를 가져옵니다."""

    if market == "KRX":
        df = fdr.DataReader(f"NAVER:{symbol}", start=start_date)
    elif market == "ETF/KR":
        df = fdr.DataReader(f"NAVER:{symbol}", start=start_date)
    else:
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
            df_to_save["Change"] = df_to_save["Close"].pct_change()
        cols = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "Change"]
        df_to_save = df_to_save[cols]
        df_to_save["Date"] = pd.to_datetime(df_to_save["Date"]).dt.strftime("%Y-%m-%d")
        df_to_save.fillna(0, inplace=True)

        cursor = conn.cursor()
        data_tuples = [tuple(x) for x in df_to_save.to_numpy()]
        cursor.executemany(
            f"""
            INSERT OR REPLACE INTO stock_price ({", ".join(cols)}) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            data_tuples,
        )
        conn.commit()


def process_single_symbol(symbol: str, db_path: str, market: str):
    """단일 종목에 대한 증분 업데이트를 처리하는 워커 함수."""
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
    result = fetch_stock_data(symbol, start_date, market)
    if result:
        _, df = result
        save_price_to_db(db_path, symbol, df)


def process_market_data(market: str, db_path: str) -> None:
    """지정된 시장의 모든 종목 데이터를 병렬로 가져와 DB에 저장합니다."""
    print(f"\n[{market}] 시장 정보 및 시세 수집을 시작합니다...")
    symbols_df: pd.DataFrame = fdr.StockListing(market)

    if market in "KRX":
        symbols_df.rename(columns={"Code": "Symbol"}, inplace=True)

    if symbols_df.empty:
        print(f"'{market}' 시장에서 종목 목록을 가져올 수 없습니다.")
        return

    # ⭐ 1단계: 시장의 전체 종목 정보를 별도 테이블에 저장
    save_market_info_to_db(db_path, market, symbols_df)

    # 2단계: 각 종목의 주가 데이터를 병렬로 수집
    symbol_list: List[str] = (
        symbols_df.index.tolist()
        if "Symbol" not in symbols_df.columns
        else symbols_df["Symbol"].tolist()
    )

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_single_symbol, symbol, db_path, market): symbol
            for symbol in symbol_list
        }
        for future in tqdm(
            as_completed(futures),
            total=len(symbol_list),
            desc=f"Processing Prices in {market}",
        ):
            symbol = futures[future]
            try:
                future.result(timeout=REQUEST_TIMEOUT)
            except TimeoutError:
                tqdm.write(
                    f"오류: 종목 '{symbol}' 데이터 요청 시간 초과({REQUEST_TIMEOUT}초). 건너뜁니다."
                )
            except Exception as e:
                tqdm.write(
                    f"오류: 종목 '{symbol}' 처리 중 문제 발생 - {type(e).__name__}: {e}"
                )

    print(f"[{market}] 시장 시세 데이터 수집 완료.")


def main():
    """메인 실행 함수"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILE)
    setup_database(db_path)
    for market in MARKETS:
        process_market_data(market, db_path)
    print("\n모든 시장에 대한 데이터 수집 작업이 완료되었습니다.")


if __name__ == "__main__":
    main()
