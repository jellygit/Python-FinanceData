# data_handler.py
import sqlite3
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

def load_data(db_path, tickers, start_date_str, history_months=13):
    """DB에서 시세 데이터를 로드하고, 지표 계산을 위해 충분한 과거 데이터를 포함합니다."""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        # 모멘텀 계산을 위해 최소 13개월 전 데이터부터 로드
        load_start_date = start_date - relativedelta(months=history_months)
        
        with sqlite3.connect(db_path) as con:
            placeholders = ", ".join("?" for _ in tickers)
            query = (f"SELECT Date, Symbol, Close FROM stock_prices WHERE Symbol IN ({placeholders}) AND Date >= ? ORDER BY Date")
            
            # 파라미터는 티커 리스트와 날짜 문자열
            params = list(tickers) + [load_start_date.strftime('%Y-%m-%d')]
            df = pd.read_sql_query(query, con, params=params)
            
            if df.empty:
                return pd.DataFrame()

            df.rename(columns={"Date": "date", "Symbol": "ticker", "Close": "close"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            
            # Pivot을 사용하여 날짜를 인덱스로, 티커를 컬럼으로 변환
            pivot_df = df.pivot(index="date", columns="ticker", values="close")
            # 휴일 등으로 인한 중간의 NaN 값만 이전 값으로 채움
            pivot_df = pivot_df.ffill()
            return pivot_df

    except Exception as e:
        sys.exit(f"시세 데이터 로딩 중 오류 발생: {e}")

def prepare_strategy_data(stock_data):
    """전략에 필요한 모든 지표(모멘텀, 이동평균선 등)를 미리 계산합니다."""
    print("전략 데이터 사전 계산 중 (모멘텀, 이동평균선 등)...")
    
    # --- 월별 데이터 기반 지표 ---
    monthly_prices = stock_data.resample('M').last()
    momentum_data = {}
    for period in [1, 3, 6, 12]:
        momentum_data[f'roc_{period}'] = (monthly_prices / monthly_prices.shift(period) - 1)
    
    # DAA용 가중 모멘텀
    momentum_data['daa_momentum'] = (12 * momentum_data['roc_1'] + 4 * momentum_data['roc_3'] + 2 * momentum_data['roc_6'] + 1 * momentum_data['roc_12'])
    
    # HAA용 12개월 이동평균
    momentum_data['sma_12_month'] = monthly_prices.rolling(window=12).mean()

    # --- 일별 데이터 기반 지표 ---
    daily_data = {}
    if 'SPY' in stock_data.columns:
        # LAA용 200일 이동평균
        daily_data['sma_200_day'] = stock_data['SPY'].rolling(window=200).mean()

    return monthly_prices, momentum_data, daily_data

def load_dividends_data(db_path, tickers):
    """[신규] DB에서 지정된 티커들의 배당 데이터를 로드합니다."""
    print("배당 데이터 로딩 중...")
    dividends_by_ticker = {}
    try:
        with sqlite3.connect(db_path) as con:
            placeholders = ", ".join("?" for _ in tickers)
            query = f"SELECT Date, Symbol, Dividend FROM stock_dividends WHERE Symbol IN ({placeholders}) ORDER BY Date"
            
            df = pd.read_sql_query(query, con, params=list(tickers))

            if not df.empty:
                df['Date'] = pd.to_datetime(df['Date'])
                # 각 티커별로 데이터를 그룹화하여 딕셔너리에 저장
                for ticker, group in df.groupby('Symbol'):
                    # 날짜를 인덱스로 설정하여 빠른 조회를 위함
                    dividends_by_ticker[ticker] = group.set_index('Date')['Dividend']
        
        return dividends_by_ticker
    except Exception as e:
        # 배당 테이블이 없거나 해도 오류 없이 빈 딕셔너리 반환
        print(f"경고: 배당 데이터를 불러오는 중 문제 발생 (무시하고 진행): {e}", file=sys.stderr)
        return {}
