#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
포트폴리오 백테스팅 시뮬레이터 (Interval 로직 수정)
"""
import argparse
import sqlite3
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# --- 거래 비용 정의 ---
BUY_COMMISSION_RATE = 0.0025  # 매수 시 수수료 0.25%
SELL_TAX_RATE = 0.0025      # 매도 시 세금/비용 0.25%


# --- 전략별 자산 목록 정의 ---
STRATEGY_ASSETS = {
    'haa': {
        'offensive': ['SPY', 'QQQ'],
        'defensive': ['IEF', 'BIL'],
        'canary': ['SPY']
    },
    'daa': {
        'offensive': ['SPY', 'IWM', 'QQQ', 'VGK', 'EWJ', 'VWO', 'VNQ', 'GSG', 'GLD', 'TLT', 'HYG', 'LQD'],
        'defensive': ['IEF', 'LQD', 'TLT'],
        'canary': ['SPY', 'EEM', 'EFA', 'AGG']
    },
    'laa': {
        'offensive': ['SPY', 'QQQ', 'EFA', 'EEM', 'AGG'],
        'defensive': ['IEF'],
        'canary': ['SPY', 'EFA', 'EEM', 'AGG']
    }
}


def load_data(db_path, tickers, start_date_str, history_months=13):
    """DB에서 데이터를 로드하고, 모멘텀 계산을 위해 충분한 과거 데이터를 포함합니다."""
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        load_start_date = start_date - relativedelta(months=history_months)
        
        with sqlite3.connect(db_path) as con:
            placeholders = ", ".join("?" for _ in tickers)
            query = (f"SELECT Date, Symbol, Close FROM stock_price WHERE Symbol IN ({placeholders}) AND Date >= ? ORDER BY Date")
            df = pd.read_sql_query(query, con, params=list(tickers) + [load_start_date.strftime('%Y-%m-%d')])
            df.rename(columns={"Date": "date", "Symbol": "ticker", "Close": "close"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"])
            
            pivot_df = df.pivot(index="date", columns="ticker", values="close")
            pivot_df = pivot_df.ffill()
            return pivot_df

    except Exception as e:
        sys.exit(f"데이터 로딩 중 오류 발생: {e}")


def prepare_momentum_data(stock_data):
    """주가 데이터를 바탕으로 월별 수익률, 이동평균선 등 필요한 모든 지표를 미리 계산합니다."""
    print("모멘텀 데이터 사전 계산 중...")
    monthly_prices = stock_data.resample('M').last()
    
    momentum_data = {}
    for period in [1, 3, 6, 12]:
        momentum_data[f'roc_{period}'] = (monthly_prices / monthly_prices.shift(period) - 1)

    momentum_data['daa_momentum'] = (12 * momentum_data['roc_1'] + 
                                     4 * momentum_data['roc_3'] + 
                                     2 * momentum_data['roc_6'] + 
                                     1 * momentum_data['roc_12'])
    
    momentum_data['laa_momentum'] = (momentum_data['roc_1'] + 
                                     momentum_data['roc_3'] + 
                                     momentum_data['roc_6'] + 
                                     momentum_data['roc_12']) / 4.0

    momentum_data['sma_12'] = monthly_prices.rolling(window=12).mean()

    return monthly_prices, momentum_data


def decide_haa_portfolio(date, monthly_prices, momentum_data):
    """HAA 전략에 따라 목표 포트폴리오를 결정합니다."""
    assets = STRATEGY_ASSETS['haa']
    roc_6 = momentum_data['roc_6'].loc[date]
    sma_12 = momentum_data['sma_12'].loc[date]
    
    offensive_pick = roc_6[assets['offensive']].idxmax()
    defensive_pick = roc_6[assets['defensive']].idxmax()
    
    canary_price = monthly_prices.loc[date, assets['canary'][0]]
    canary_sma = sma_12[assets['canary'][0]]

    if pd.isna(canary_price) or pd.isna(canary_sma): return {}

    if canary_price > canary_sma:
        return {offensive_pick: 1.0}
    else:
        return {defensive_pick: 1.0}


def decide_daa_portfolio(date, momentum_data):
    """DAA 전략에 따라 목표 포트폴리오를 결정합니다."""
    assets = STRATEGY_ASSETS['daa']
    daa_momentum = momentum_data['daa_momentum'].loc[date]
    
    canary_scores = daa_momentum[assets['canary']]
    if canary_scores.isnull().all() or canary_scores.mean() < 0:
        defensive_pick = daa_momentum[assets['defensive']].idxmax()
        return {defensive_pick: 1.0}
    else:
        offensive_picks = daa_momentum[assets['offensive']].nlargest(3).index
        return {ticker: 1.0 / len(offensive_picks) for ticker in offensive_picks}


def decide_laa_portfolio(date, momentum_data):
    """LAA 전략에 따라 목표 포트폴리오를 결정합니다."""
    assets = STRATEGY_ASSETS['laa']
    laa_momentum = momentum_data['laa_momentum'].loc[date]
    
    num_positive_canary = (laa_momentum[assets['canary']] > 0).sum()
    
    if num_positive_canary >= len(assets['canary']) / 2.0:
        offensive_pick = laa_momentum[assets['offensive']].idxmax()
        return {offensive_pick: 1.0}
    else:
        return {assets['defensive'][0]: 1.0}


def execute_rebalancing(holdings, cash, target_portfolio, prices):
    """거래 비용을 고려하여 현재 포트폴리오를 목표 포트폴리오로 변경합니다."""
    
    current_portfolio_value = cash + sum(holdings.get(t, 0) * prices.get(t, 0) for t in holdings)
    
    tickers_to_sell = [t for t in holdings if holdings[t] > 0 and t not in target_portfolio]
    for ticker in tickers_to_sell:
        shares = holdings[ticker]
        price = prices.get(ticker)
        if shares > 0 and price is not None:
            holdings[ticker] = 0
            base_proceeds = shares * price
            tax = base_proceeds * SELL_TAX_RATE
            net_proceeds = base_proceeds - tax
            cash += net_proceeds
            print(f"- {ticker}: {shares:,}주 전량 매도 (금액: {base_proceeds:,.2f}, 비용: {tax:,.2f})")
            
    for ticker, target_weight in target_portfolio.items():
        target_value = current_portfolio_value * target_weight
        current_value = holdings.get(ticker, 0) * prices.get(ticker, 0)
        delta_value = target_value - current_value
        price = prices.get(ticker)
        
        if price is None or price <= 0: continue
        
        if delta_value > 0:
            shares_to_buy = int(delta_value / (price * (1 + BUY_COMMISSION_RATE)))
            if shares_to_buy > 0:
                base_cost = shares_to_buy * price
                commission = base_cost * BUY_COMMISSION_RATE
                total_cost = base_cost + commission
                if cash >= total_cost:
                    holdings[ticker] = holdings.get(ticker, 0) + shares_to_buy
                    cash -= total_cost
                    print(f"- {ticker}: {shares_to_buy:,}주 매수/추가매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})")
        elif delta_value < 0:
            shares_to_sell = int(-delta_value / price)
            if shares_to_sell > 0 and holdings.get(ticker, 0) >= shares_to_sell:
                holdings[ticker] -= shares_to_sell
                base_proceeds = shares_to_sell * price
                tax = base_proceeds * SELL_TAX_RATE
                net_proceeds = base_proceeds - tax
                cash += net_proceeds
                print(f"- {ticker}: {shares_to_sell:,}주 비중조절 매도 (금액: {base_proceeds:,.2f}, 비용: {tax:,.2f})")
    
    return holdings, cash


def evaluate_portfolio_state(date, holdings, cash, prices, all_tickers):
    """특정 시점의 포트폴리오 상태를 평가하여 딕셔너리로 반환합니다."""
    eval_result = {"Date": date.strftime("%Y-%m-%d"), "Cash": cash}
    total_value = cash
    
    for ticker in all_tickers:
        price = prices.get(ticker, 0.0)
        stock_value = holdings.get(ticker, 0) * price
        total_value += stock_value
        eval_result[f"{ticker} Holdings"] = holdings.get(ticker, 0)
        eval_result[f"{ticker} Price"] = price
        eval_result[f"{ticker} Value"] = stock_value

    for ticker in all_tickers:
        stock_value = eval_result[f"{ticker} Value"]
        eval_result[f"{ticker} Weight"] = stock_value / total_value if total_value > 0 else 0
        
    return eval_result


def generate_plot(plot_df, all_tickers):
    """시뮬레이션 결과를 바탕으로 그래프를 생성하고 파일로 저장합니다."""
    plot_df.index = pd.to_datetime(plot_df.index)
    fig, ax1 = plt.subplots(figsize=(18, 9))
    ax1.set_yscale('log')
    ax1.plot(plot_df.index, plot_df['Portfolio Value'], label='Portfolio Value', color='royalblue', linewidth=2)
    ax1.plot(plot_df.index, plot_df['Total Investment'], label='Total Investment', color='red', linestyle='--', linewidth=2)
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Amount (Log Scale)')
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{int(x):,}"))
    ax1.yaxis.set_minor_formatter(mticker.FuncFormatter(lambda x, p: f"{int(x):,}"))
    ax2 = ax1.twinx()
    weight_columns = [f"{ticker} Weight" for ticker in all_tickers]
    ax2.stackplot(plot_df.index, plot_df[weight_columns].T, labels=weight_columns, alpha=0.3)
    ax2.set_ylabel('Asset Weight (%)')
    ax2.set_ylim(0, 1)
    ax2.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    plt.title('Portfolio Value and Asset Allocation Over Time', fontsize=16)
    fig.legend(loc='upper left', bbox_to_anchor=(0.1, 0.9))
    ax1.grid(True, which="both", ls="--", linewidth=0.5)
    plt.setp(ax1.get_yticklabels(minor=True), fontsize='small')
    fig.tight_layout()
    filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.webp'
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"\n📈 그래프를 '{filename}' 파일로 저장했습니다.")


def main():
    parser = argparse.ArgumentParser(description="포트폴리오 백테스팅 시뮬레이터 (전략: 기본, HAA, DAA, LAA)")
    parser.add_argument("capital", type=float, help="초기 투자금")
    parser.add_argument("stocks", nargs="*", help="[기본 전략용] 티커와 비중 목록 (예: SPY 0.6 AGG 0.4)")
    parser.add_argument("--start-date", required=True, help="시뮬레이션 시작일 (YYYY-MM-DD)")
    parser.add_argument("--db-path", required=True, help="SQLite 데이터베이스 파일 경로")
    parser.add_argument("--strategy", default="default", choices=['default', 'haa', 'daa', 'laa'], help="투자 전략 선택")
    parser.add_argument("--interval", default="3M", help="리밸런싱 주기 (예: 1M, 3M). HAA/DAA/LAA는 보통 1M을 권장합니다.")
    parser.add_argument("--periodic-investment", "-pi", type=float, default=0.0, help="주기별 추가 투자금액")
    args = parser.parse_args()

    if args.strategy == 'default':
        if len(args.stocks) < 2 or len(args.stocks) % 2 != 0:
            sys.exit("기본(default) 전략을 사용하려면 티커와 비중을 쌍으로 입력해야 합니다.")
        all_tickers = set(args.stocks[::2])
        target_weights = {t: float(w) for t, w in zip(args.stocks[::2], args.stocks[1::2])}
    else:
        assets = STRATEGY_ASSETS[args.strategy]
        all_tickers = set(assets['offensive']) | set(assets['defensive']) | set(assets['canary'])
        target_weights = {}

    stock_data = load_data(args.db_path, all_tickers, args.start_date)
    monthly_prices, momentum_data = prepare_momentum_data(stock_data)

    sim_start_date = pd.to_datetime(args.start_date)
    
    # [수정] 사용자의 interval에 맞춰 평가일을 생성하는 로직
    # 1. 사용자가 지정한 간격으로 이론적인 평가일 생성
    theoretical_dates = pd.date_range(
        start=sim_start_date,
        end=monthly_prices.index[-1],
        freq=args.interval
    )
    # 2. 이론적인 평가일과 가장 가까운, 실제 데이터가 있는 월말 날짜를 찾음
    #    (searchsorted는 날짜 배열에서 해당 날짜가 들어갈 위치를 찾아줌)
    date_indices = monthly_prices.index.searchsorted(theoretical_dates, side='left')
    # 중복된 날짜 제거 (예: 30일, 31일이 모두 같은 월말로 처리될 경우)
    unique_indices = sorted(list(set(date_indices)))
    evaluation_dates = monthly_prices.index[unique_indices]
    # 사용자가 지정한 시작일보다 빠른 날짜는 최종적으로 제외
    evaluation_dates = evaluation_dates[evaluation_dates >= sim_start_date]

    cash = args.capital
    holdings = {ticker: 0 for ticker in all_tickers}
    total_investment = args.capital
    results = []

    for i, date in enumerate(evaluation_dates):
        print(f"\n--- 평가일: {date.strftime('%Y-%m-%d')} ---")
        
        if i == 0:
             print("시뮬레이션을 시작합니다. 초기 자본으로 포트폴리오를 구성합니다.")
        else:
            if args.periodic_investment > 0:
                cash += args.periodic_investment
                total_investment += args.periodic_investment
                print(f"✅ 추가 투자금 입금: {args.periodic_investment:,.2f} | 조정 후 현금: {cash:,.2f}")
            
        current_prices = stock_data.loc[stock_data.index.asof(date)]

        if args.strategy == 'haa':
            target_portfolio = decide_haa_portfolio(date, monthly_prices, momentum_data)
        elif args.strategy == 'daa':
            target_portfolio = decide_daa_portfolio(date, momentum_data)
        elif args.strategy == 'laa':
            target_portfolio = decide_laa_portfolio(date, momentum_data)
        else: 
            target_portfolio = target_weights

        if not target_portfolio:
            print("목표 포트폴리오를 결정할 수 없어 현재 상태를 유지합니다.")
        else:
            if i == 0:
                 initial_portfolio_value = cash
                 for ticker, weight in target_portfolio.items():
                     price = current_prices.get(ticker)
                     if price is not None and price > 0:
                         allocation = initial_portfolio_value * weight
                         shares_to_buy = int(allocation / (price * (1 + BUY_COMMISSION_RATE)))
                         if shares_to_buy > 0:
                            base_cost = shares_to_buy * price
                            commission = base_cost * BUY_COMMISSION_RATE
                            total_cost = base_cost + commission
                            if cash >= total_cost:
                                holdings[ticker] = shares_to_buy
                                cash -= total_cost
                                print(f"- {ticker}: {shares_to_buy:,}주 초기 매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})")
            else:
                holdings, cash = execute_rebalancing(holdings.copy(), cash, target_portfolio, current_prices)

        eval_result = evaluate_portfolio_state(date, holdings, cash, current_prices, all_tickers)
        eval_result['Total Investment'] = total_investment
        results.append(eval_result)
        
    if results:
        numeric_df = pd.DataFrame(results).fillna(0)
        numeric_df.set_index("Date", inplace=True)
        value_columns = [f"{t} Value" for t in all_tickers if f"{t} Value" in numeric_df.columns]
        numeric_df["Portfolio Value"] = numeric_df[value_columns].sum(axis=1) + numeric_df["Cash"]
        
        generate_plot(numeric_df.copy(), list(all_tickers))

        display_df = numeric_df.copy()
        for ticker in all_tickers:
            for col_type in ["Weight", "Value", "Price"]:
                col_name = f"{ticker} {col_type}"
                if col_name in display_df.columns:
                    display_df[col_name] = display_df[col_name].apply(
                        lambda x: f"{x:.2%}" if col_type == "Weight" else f"{x:,.2f}"
                    )
        for col in ["Portfolio Value", "Total Investment", "Cash"]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}")
                
        display_columns = ["Portfolio Value", "Total Investment", "Cash"]
        for ticker in all_tickers:
            display_columns.extend([f"{ticker} Holdings", f"{ticker} Price", f"{ticker} Value", f"{ticker} Weight"])
        
        display_columns = [col for col in display_columns if col in display_df.columns]
        
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print("\n--- 포트폴리오 가치 평가 결과 ---")
            print(display_df[display_columns])


if __name__ == "__main__":
    main()
