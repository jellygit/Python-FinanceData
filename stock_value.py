#!/usr/bin/env python
"""
python get_stock_value.py 종목코드
적으면 표준 출력으로 결과 나옴.
"""
import os
import sys
import datetime as dt
import pandas as pd
import sqlite3

# 수수료 계산 함수.
# 수수료는 매수/매도 시, Tax on sale. KOSPI 0.25, KOSDAQ 0.25
def F_FEE(Get_Date):
    FEE_STD = dt.datetime(2019, 6, 1, 0, 0, 0)
    FEE_STD = FEE_STD.__format__("%Y-%m-%d %H:%M:%S")
    if Get_Date > FEE_STD:
        return 0.13
    else:
        return 0.18

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

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

CONN = create_connection('./db/finance.db')
BT = create_connection('./db/backtest.db')

# SYM = '005930'
COUNTRY = "KR"
SYM = sys.argv[1]
SYM = str(SYM)
# COUNTRY = sys.argv[2]

# SYM 변수가 finance.db에 없으면 에러 발생..
DF = pd.read_sql('select * from \"' + SYM + '\"', con = CONN, index_col="Date")

# Create Backtest Table.
# if exist, read latest record.
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = '{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    dbcon.commit()

    #commit the changes to db			
    dbcur.close()
    return False

if (checkTableExists(BT, SYM)):
    print("Table is exist")
else:
    print("Table is not exist, Create Table " + SYM)
    create_table(BT, SYM)

# 매월 투입금
BUDGET = 500000
# 거스름돈
WALLET = 0
# 보유주식 수
STOCK = 0
# 평가액
PRINCIPAL = 0

"""
for year in range(2019, 2021):
    for month in range(1, 13):
        for day in range(1, 28):
            date = dt.datetime(year, month, day, 0, 0, 0)
            date = date.__format__("%Y-%m-%d %H:%M:%S")

            if date in DF.index:
                # 주식 매수
                STOCK = (BUDGET + WALLET) // ((DF.at[date, 'Close'])) + STOCK
                WALLET = (BUDGET + WALLET) % ((DF.at[date, 'Close']))
                PRINCIPAL += BUDGET
                VALUATION = DF.at[date, 'Close'] * STOCK + WALLET
                RATE = ((VALUATION / PRINCIPAL) - 1)
                print('{}	{}	{}	{}	{}	{:.2}	{}'.format(
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
"""
CONN.close()
BT.close()
