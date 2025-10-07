#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
포트폴리오 백테스팅 시뮬레이터 (메인 실행 파일)
"""
import argparse
import sys
import pandas as pd
from config import STRATEGY_ASSETS, BUY_COMMISSION_RATE
from data_handler import load_data, prepare_strategy_data
from strategies import decide_haa_portfolio, decide_daa_portfolio, decide_laa_portfolio
from portfolio_manager import execute_rebalancing, evaluate_portfolio_state
from reporting import print_final_report, generate_plot


def main():
    parser = argparse.ArgumentParser(description="포트폴리오 백테스팅 시뮬레이터")
    parser.add_argument("capital", type=float, help="초기 투자금")
    parser.add_argument("stocks", nargs="*", help="[기본 전략용] 티커와 비중 목록 (예: SPY 0.6 AGG 0.4)")
    parser.add_argument("--start-date", required=True, help="시뮬레이션 시작일 (YYYY-MM-DD)")
    parser.add_argument("--db-path", required=True, help="SQLite 데이터베이스 파일 경로")
    parser.add_argument("--strategy", default="default", choices=['default', 'haa', 'daa', 'laa'], help="투자 전략 선택")
    parser.add_argument("--interval", default="1M", help="리밸런싱 주기 (예: 1M, 3M)")
    parser.add_argument("--periodic-investment", "-pi", type=float, default=0.0, help="주기별 추가 투자금액")
    args = parser.parse_args()

    # 1. 전략에 따른 자산 목록 및 설정 준비
    if args.strategy == 'default':
        if len(args.stocks) < 2 or len(args.stocks) % 2 != 0:
            sys.exit("기본(default) 전략을 사용하려면 티커와 비중을 쌍으로 입력해야 합니다.")
        all_tickers = set(args.stocks[::2])
        target_weights = {t: float(w) for t, w in zip(args.stocks[::2], args.stocks[1::2])}
    else:
        assets = STRATEGY_ASSETS[args.strategy]
        all_tickers = set.union(*[set(v) for v in assets.values()])
        target_weights = {}

    # 2. 데이터 로딩 및 지표 사전 계산
    stock_data = load_data(args.db_path, all_tickers, args.start_date)
    monthly_prices, momentum_data, daily_data = prepare_strategy_data(stock_data)

    # 3. 시뮬레이션 날짜 생성
    sim_start_date = pd.to_datetime(args.start_date)
    theoretical_dates = pd.date_range(start=sim_start_date, end=monthly_prices.index[-1], freq=args.interval)
    date_indices = monthly_prices.index.searchsorted(theoretical_dates, side='left')
    unique_indices = sorted(list(set(date_indices)))
    evaluation_dates = monthly_prices.index[unique_indices]
    evaluation_dates = evaluation_dates[evaluation_dates >= sim_start_date]

    # 4. 시뮬레이션 시작
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

        # 5. 전략에 따른 목표 포트폴리오 결정
        if args.strategy == 'haa':
            target_portfolio = decide_haa_portfolio(date, monthly_prices, momentum_data)
        elif args.strategy == 'daa':
            target_portfolio = decide_daa_portfolio(date, momentum_data)
        elif args.strategy == 'laa':
            target_portfolio = decide_laa_portfolio(date, current_prices, daily_data)
        else: 
            target_portfolio = target_weights

        if not target_portfolio:
            print("목표 포트폴리오를 결정할 수 없어 현재 상태를 유지합니다.")
        else:
            # 6. 거래 실행 (초기 매수 또는 리밸런싱)
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

        # 7. 최종 상태 평가 및 기록
        eval_result = evaluate_portfolio_state(date, holdings, cash, current_prices, all_tickers)
        eval_result['Total Investment'] = total_investment
        results.append(eval_result)
        
    # 8. 최종 결과 처리 및 출력
    if results:
        numeric_df = pd.DataFrame(results).fillna(0)
        numeric_df.set_index("Date", inplace=True)
        
        # [수정] 모든 계산을 여기서 수행
        value_columns = [f"{t} Value" for t in all_tickers if f"{t} Value" in numeric_df.columns]
        numeric_df["Portfolio Value"] = numeric_df[value_columns].sum(axis=1) + numeric_df["Cash"]
        
        # ROI 계산
        numeric_df['ROI'] = (numeric_df['Portfolio Value'] - numeric_df['Total Investment']) / numeric_df['Total Investment']
        
        # 그래프 및 텍스트 리포트 생성
        generate_plot(numeric_df.copy(), list(all_tickers))
        print_final_report(numeric_df.copy(), list(all_tickers))


if __name__ == "__main__":
    main()
