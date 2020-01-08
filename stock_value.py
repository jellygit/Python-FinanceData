#!/usr/bin/env python
"""
python get_stock_value.py 종목코드
적으면 표준 출력으로 결과 나옴.
"""
import datetime as dt
import pandas as pd
import sqlite3

def F_FEE(Get_Date):
    FEE_STD = dt.datetime(2019, 6, 1, 0, 0, 0)
    FEE_STD = FEE_STD.__format__("%Y-%m-%d %H:%M:%S")
    if Get_Date > FEE_STD:
        return 0.13
    else:
        return 0.18

CONN = sqlite3.connect('./db/finance.db')

SYM = '005930'
COUNTRY = "KR"
# SYM = sys.argv[1]
SYM = str(SYM)
# COUNTRY = sys.argv[2]

DF = pd.read_sql('select * from \"' + SYM + '\"', con = CONN, index_col="Date")

# 매월 투입금
BUDGET = 500000
# 거스름돈
WALLET = 0
# 보유주식 수
STOCK = 0
# 평가액
PRINCIPAL = 0

for year in range(2010, 2021):
    for month in range(1, 13):
        for day in range(1, 28):
            date = dt.datetime(year, month, day, 0, 0, 0)
            date = date.__format__("%Y-%m-%d %H:%M:%S")

            if date in DF.index:
                # 주식 매수
                FEE = F_FEE(date)
                STOCK = (BUDGET + WALLET) // ((DF.at[date, 'Close']) * (1 + FEE)) + STOCK
                WALLET = (BUDGET + WALLET) % ((DF.at[date, 'Close']) * ( 1 + FEE ))
                PRINCIPAL += BUDGET
                VALUATION = DF.at[date, 'Close'] * STOCK + WALLET
                RATE = ((VALUATION / PRINCIPAL) - 1)
                print(date)
                print('{},{},{},{},{},{:.2},{}'.format(
                    str(date),
                    DF.at[date, 'Close'],
                    STOCK,
                    WALLET,
                    PRINCIPAL,
                    RATE,
                    VALUATION
                    )
                    )
                break
