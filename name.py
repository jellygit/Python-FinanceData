#!/bin/env python
import os
import sys
import pandas as pd
import sqlite3

pd.set_option('display.max_rows', None)
pd.set_option('display.max_seq_items', None)

## 인자 확인: 0는 실행파일 명, 1 이후가 인자
## 비어 있으면 전체 마켓 업데이트
if len(sys.argv) > 1:
    MARKETS = sys.argv
    MARKETS.pop(0)
else:
    # MARKETS = [ "KRX", "ETF/KR" ]
    MARKETS = [ "ETF/KR", "KRX", "NASDAQ", "NYSE", "ETF/US", "AMEX" ]


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

Symbol_name = pd.DataFrame()
for MARKET in MARKETS:
    CONN = create_connection('./db/finance.db')
    df = pd.read_sql('select Symbol, Name from \"'+ MARKET +'"', con = CONN)
    Symbol_name = pd.concat([Symbol_name, df])

print(Symbol_name)

CONN.close()
