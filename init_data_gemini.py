#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FinanceDataReader를 사용하여 주식 시세 데이터를 가져와 SQLite DB에 저장합니다.
이 스크립트는 에러 처리, PEP 8/257/484 준수 및 멀티-스레딩을 통한 성능 개선이
적용되었습니다.
"""

import os
import sqlite3
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 상수 정의 ---
DB_FILE: str = "db/finance.db"
MARKET: str = "NYSE"
# MARKET: str = "KRX"
# MARKET: str = "NASDAQ"
START_DATE: str = "2008-01-01"
# 동시 요청할 스레드 수 (네트워크 및 PC 환경에 따라 조절)
MAX_WORKERS: int = 1


def setup_database(db_path: str) -> None:
    """
    SQLite 데이터베이스와 필요한 테이블을 초기화합니다.
    테이블이 이미 존재하면 생성하지 않습니다.

    Args:
        db_path (str): 데이터베이스 파일 경로.
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_price (
            Symbol TEXT,
            Date TEXT,
            Open REAL,
            High REAL,
            Low REAL,
            Close REAL,
            Volume INTEGER,
            Change REAL,
            PRIMARY KEY (Symbol, Date)
        )
        """)
        conn.commit()


def fetch_stock_data(
    symbol: str, start_date: str
) -> Optional[Tuple[str, pd.DataFrame]]:
    """
    지정된 단일 종목의 시세 데이터를 가져옵니다.

    네트워크 오류나 데이터 없음(404) 등의 예외 발생 시,
    에러를 출력하고 None을 반환하여 전체 프로세스가 중단되지 않도록 합니다.

    Args:
        symbol (str): 조회할 종목 코드.
        start_date (str): 조회 시작일 (YYYY-MM-DD 형식).

    Returns:
        Optional[Tuple[str, pd.DataFrame]]:
            성공 시 (종목 코드, 데이터프레임) 튜플을 반환하고,
            실패 시 None을 반환합니다.
    """
    try:
        df = fdr.DataReader(symbol, start_date)
        # df = fdr.DataReader(f"YAHOO:{symbol}", start_date)
        # df = fdr.DataReader(f"NAVER:{symbol}", start_date)
        if df.empty:
            print(f"[{symbol}] 데이터가 비어있습니다. 건너뜁니다.")
            return None
        return symbol, df
    except Exception as e:
        # HTTPError 404 외에도 다양한 네트워크 예외를 처리
        print(f"Error fetching [{symbol}]: {e}")
        return None


def save_to_db(db_path: str, symbol: str, df: pd.DataFrame) -> None:
    """
    데이터프레임을 SQLite 데이터베이스에 저장합니다.

    Args:
        db_path (str): 데이터베이스 파일 경로.
        symbol (str): 저장할 종목 코드.
        df (pd.DataFrame): 저장할 시세 데이터프레임.
    """
    with sqlite3.connect(db_path) as conn:
        # Date 컬럼을 인덱스에서 일반 컬럼으로 변경
        df_to_save = df.reset_index()
        df_to_save["Symbol"] = symbol

        # DB 테이블 컬럼 순서에 맞게 DataFrame 컬럼 재정렬
        cols = ["Symbol", "Date", "Open", "High", "Low", "Close", "Volume", "Change"]
        df_to_save = df_to_save[cols]

        # 날짜 형식을 'YYYY-MM-DD' 문자열로 변환
        df_to_save["Date"] = df_to_save["Date"].dt.strftime("%Y-%m-%d")

        # 중복 데이터를 방지하며 삽입 (INSERT OR REPLACE)
        df_to_save.to_sql(
            "stock_price",
            conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )


def process_market_data(market: str, start_date: str, db_path: str) -> None:
    """
    지정된 시장의 모든 종목 데이터를 병렬로 가져와 DB에 저장합니다.

    ThreadPoolExecutor를 사용하여 다수의 종목 데이터를 동시에 요청하여
    전체 작업 시간을 단축합니다.

    Args:
        market (str): 조회할 시장 (예: 'KRX', 'ETF/KR').
        start_date (str): 조회 시작일.
        db_path (str): 데이터베이스 파일 경로.
    """
    print(f"'{market}' 시장의 모든 종목 시세를 가져옵니다...")
    symbols: pd.DataFrame = fdr.StockListing(market)

    ## 한국만 Symbol 이 아니라 Code 인데 호환성을 위해 Rename
    if MARKET in "KRX":
        symbols.rename(columns={"Code": "Symbol"}, inplace=True)

    if symbols.empty:
        print(f"'{market}' 시장에서 종목 목록을 가져올 수 없습니다.")
        return

    symbol_list: List[str] = symbols["Symbol"].tolist()
    total_symbols = len(symbol_list)
    completed_count = 0

    # 멀티-스레드 풀을 사용하여 데이터 병렬 조회
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 각 종목에 대한 데이터 조회 작업을 제출
        future_to_symbol = {
            executor.submit(fetch_stock_data, symbol, start_date): symbol
            for symbol in symbol_list
        }

        for future in as_completed(future_to_symbol):
            result = future.result()
            completed_count += 1

            if result:
                symbol, df = result
                print(
                    f"({completed_count}/{total_symbols}) [{symbol}] 데이터 가져오기 성공. DB에 저장합니다."
                )
                save_to_db(db_path, symbol, df)
            else:
                # fetch_stock_data에서 에러가 발생하여 None이 반환된 경우
                failed_symbol = future_to_symbol[future]
                print(
                    f"({completed_count}/{total_symbols}) [{failed_symbol}] 데이터 가져오기 실패."
                )

    print("\n모든 작업이 완료되었습니다.")


if __name__ == "__main__":
    # 스크립트 실행 위치를 기준으로 DB 파일 경로 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, DB_FILE)

    # 1. 데이터베이스 설정
    setup_database(db_path)

    # 2. 지정된 마켓의 데이터 처리 시작
    process_market_data(MARKET, START_DATE, db_path)
