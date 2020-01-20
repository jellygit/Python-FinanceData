#!/bin/env python
import os
import sys
import datetime as dt
import pandas as pd
import sqlite3
import matplotlib.ticker as mtick
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as dates

PNG_FOLDER = "png/"
if not os.path.isdir(PNG_FOLDER):
    os.mkdir(PNG_FOLDER)

# 폰트 설정
mpl.rc('font', family='NanumGothic')
# 유니코드에서  음수 부호설정
mpl.rc('axes', unicode_minus=False)

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

COUNTRY = "KR"
"""
if len(sys.argv[1]) > 1:
    SYM = sys.argv[1]
else:
    SYM = '005930'
SYM = str(SYM)
# COUNTRY = sys.argv[2]
"""

# Create Backtest Table.
# if exist, read latest record.
def checkTableExists(dbcon, tablename):
    dbcur = dbcon.cursor()
    dbcur.execute("""
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table' and name ='{0}'
        """.format(tablename.replace('\'', '\'\'')))
    if dbcur.fetchone()[0] == 1:
        dbcur.close()
        return True
    dbcon.commit()

    #commit the changes to db			
    dbcur.close()
    return False

def calcValuation(SYM):
    # SYM 변수가 finance.db에 없으면 에러 발생..
    DF = pd.read_sql('select * from \"' + SYM + '\"', con = CONN, index_col="Date")

    # 매월 투입금
    BUDGET = 500000
    # 거스름돈
    WALLET = 0
    # 보유주식 수
    STOCK = 0
    # 평가액
    PRINCIPAL = 0

    COLS_TMP = ['Date', 'Close', 'STOCK', 'WALLET', 'PRINCIPAL', 'RATE', 'VALUATION']
    RESULT_DF = pd.DataFrame(columns=COLS_TMP)

    for year in range(2010, 2021):
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
                    date = dt.datetime(year, month, day)
                    RESULT_DF = RESULT_DF.append({
                        'Date':date,
                        'PRINCIPAL':PRINCIPAL,
                        'RATE':(RATE*100),
                        'VALUATION':VALUATION},
                        ignore_index=True)
                    break
        PRINCIPAL = PRINCIPAL + (PRINCIPAL * 0.017 * 0.846)
    TITLE = pd.read_sql('select Symbol, Name from KRX where Symbol like \"' + SYM + '\"', con = CONN, index_col="Symbol")
    print(TITLE.at[SYM, 'Name'])
    print(RESULT_DF)
    RESULT_DF = RESULT_DF.set_index('Date')
    PLOT = RESULT_DF.plot(secondary_y=['RATE'], mark_right=False)
    # ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    PLOT.set_title(SYM + ' ' + TITLE.at[SYM, 'Name'])
    PLOT.right_ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    PLOT.ticklabel_format(axis='y', style='plain')

    # 그래프 파일 저장
    CSV_FIG = PNG_FOLDER + '%s.png' % (SYM)
    FIG = PLOT.get_figure()
    FIG.savefig(CSV_FIG)

for SYM in sys.argv[1:]:
    if (checkTableExists(CONN, SYM)):
        calcValuation(SYM)
    else:
        print("Table is not exist, Create Table " + SYM)
        # create_table(CONN, SYM)

CONN.close()
BT.close()
