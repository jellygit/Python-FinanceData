#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
TEN_종목코드.csv 파일 불러들여 그래프 파일 생성
"""
import os
import sys
import pandas as pd
from matplotlib import font_manager, rc

FONT_NAME = font_manager.FontProperties(
        fname='/usr/share/fonts/TTF/NanumGothic.ttf').get_name()
# 한글 폰트 지정
rc('font', family=FONT_NAME)

# 유니코드에서  음수 부호설정
rc('axes', unicode_minus=False)


CODE_NAME = pd.read_csv(
        'all_code.csv',
        parse_dates=True,
        index_col='Symbol',
        dtype={'Symbol': str}
        )
# CODE_NAME.index = [f'{x:06}' for x in CODE_NAME.index]


CSV_FOLDER = "csv/"
if not os.path.isdir(CSV_FOLDER):
    os.mkdir(CSV_FOLDER)

PNG_FOLDER = "png/"
if not os.path.isdir(PNG_FOLDER):
    os.mkdir(PNG_FOLDER)

# DF = pd.DataFrame()
SYM = sys.argv[1]

# LISTING = fdr.StockListing(SYM)

SYMBOL = CODE_NAME.loc[SYM].Name
TITLE = SYM + ' ' + SYMBOL
CSV_FN = CSV_FOLDER + 'TEN_' + '%s.csv' % (SYM)
CSV_FIG = PNG_FOLDER + '%s.png' % (SYM)
DF = pd.read_csv(CSV_FN, parse_dates=True, index_col='date')
DF = DF.drop(['주가', '보유주수', '거스름'], axis=1)
PLOT = DF.plot(secondary_y=['수익률'], mark_right=False)
PLOT.set_title(TITLE)
PLOT.set_xlabel('기간')
PLOT.set_ylabel('금액')
PLOT.right_ax.set_ylabel('BM 대비(%)')
FIG = PLOT.get_figure()
FIG.savefig(CSV_FIG)
