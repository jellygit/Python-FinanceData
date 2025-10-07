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
from portfolio_manager import (
    execute_rebalancing,
    evaluate_portfolio_state,
    execute_periodic_buy,
)
from reporting import print_final_report, generate_plot


def main():
    parser = argparse.ArgumentParser(description="포트폴리오 백테스팅 시뮬레이터")
    parser.add_argument("capital", type=float, help="초기 투자금")
    parser.add_argument(
        "stocks", nargs="*", help="[기본 전략용] 티커와 비중 목록 (예: SPY 0.6 AGG 0.4)"
    )
    # ... (다른 인자들은 동일) ...
    parser.add_argument(
        "--start-date", required=True, help="시뮬레이션 시작일 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--db-path", required=True, help="SQLite 데이터베이스 파일 경로"
    )
    parser.add_argument(
        "--strategy",
        default="default",
        choices=["default", "haa", "daa", "laa"],
        help="투자 전략 선택",
    )
    parser.add_argument("--interval", default="1M", help="리밸런싱 주기 (예: 1M, 3M)")
    parser.add_argument(
        "--periodic-investment",
        "-pi",
        type=float,
        default=0.0,
        help="주기별 추가 투자금액",
    )
    parser.add_argument(
        "--no-rebalance",
        action="store_true",
        help="[기본 전략용] 리밸런싱(매도) 없이 추가 매수만 진행",
    )
    args = parser.parse_args()

    # --- [수정] 1. 전략 & 그래프 제목 준비 ---
    graph_title = ""
    if args.strategy == "default":
        if len(args.stocks) < 2 or len(args.stocks) % 2 != 0:
            sys.exit(
                "기본(default) 전략을 사용하려면 티커와 비중을 쌍으로 입력해야 합니다."
            )
        all_tickers = set(args.stocks[::2])
        target_weights = {
            t: float(w) for t, w in zip(args.stocks[::2], args.stocks[1::2])
        }
        # default 전략의 제목 생성
        title_parts = [
            f"{ticker}: {weight:.0%}" for ticker, weight in target_weights.items()
        ]
        graph_title = ", ".join(title_parts)
    else:
        if args.no_rebalance:
            sys.exit("--no-rebalance 옵션은 default 전략에서만 사용할 수 있습니다.")
        assets = STRATEGY_ASSETS[args.strategy]
        all_tickers = set.union(*[set(v) for v in assets.values()])
        target_weights = {}
        # 동적 전략의 제목 생성
        graph_title = f"{args.strategy.upper()} Strategy"

    # ... (나머지 로직은 동일) ...
    stock_data = load_data(args.db_path, all_tickers, args.start_date)
    monthly_prices, momentum_data, daily_data = prepare_strategy_data(stock_data)
    sim_start_date = pd.to_datetime(args.start_date)
    theoretical_dates = pd.date_range(
        start=sim_start_date, end=monthly_prices.index[-1], freq=args.interval
    )
    date_indices = monthly_prices.index.searchsorted(theoretical_dates, side="left")
    unique_indices = sorted(list(set(date_indices)))
    evaluation_dates = monthly_prices.index[unique_indices]
    evaluation_dates = evaluation_dates[evaluation_dates >= sim_start_date]
    cash = args.capital
    holdings = {ticker: 0 for ticker in all_tickers}
    total_investment = args.capital
    results = []
    for i, date in enumerate(evaluation_dates):
        print(f"\n--- 평가일: {date.strftime('%Y-%m-%d')} ---")
        if i > 0 and args.periodic_investment > 0:
            cash += args.periodic_investment
            total_investment += args.periodic_investment
            print(
                f"✅ 추가 투자금 입금: {args.periodic_investment:,.2f} | 조정 후 현금: {cash:,.2f}"
            )
        current_prices = stock_data.loc[stock_data.index.asof(date)]
        if args.strategy == "haa":
            target_portfolio = decide_haa_portfolio(date, monthly_prices, momentum_data)
        elif args.strategy == "daa":
            target_portfolio = decide_daa_portfolio(date, momentum_data)
        elif args.strategy == "laa":
            target_portfolio = decide_laa_portfolio(date, current_prices, daily_data)
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
                        shares_to_buy = int(
                            allocation / (price * (1 + BUY_COMMISSION_RATE))
                        )
                        if shares_to_buy > 0:
                            base_cost = shares_to_buy * price
                            commission = base_cost * BUY_COMMISSION_RATE
                            total_cost = base_cost + commission
                            if cash >= total_cost:
                                holdings[ticker] = shares_to_buy
                                cash -= total_cost
                                print(
                                    f"- {ticker}: {shares_to_buy:,}주 초기 매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})"
                                )
            else:
                if args.strategy == "default" and args.no_rebalance:
                    holdings, cash = execute_periodic_buy(
                        holdings.copy(), cash, target_weights, current_prices
                    )
                else:
                    holdings, cash = execute_rebalancing(
                        holdings.copy(), cash, target_portfolio, current_prices
                    )
        eval_result = evaluate_portfolio_state(
            date, holdings, cash, current_prices, all_tickers
        )
        eval_result["Total Investment"] = total_investment
        results.append(eval_result)
    if results:
        numeric_df = pd.DataFrame(results).fillna(0)
        numeric_df.set_index("Date", inplace=True)
        value_columns = [
            f"{t} Value" for t in all_tickers if f"{t} Value" in numeric_df.columns
        ]
        numeric_df["Portfolio Value"] = (
            numeric_df[value_columns].sum(axis=1) + numeric_df["Cash"]
        )
        numeric_df["ROI"] = (
            numeric_df["Portfolio Value"] - numeric_df["Total Investment"]
        ) / numeric_df["Total Investment"]

        # --- [수정] 생성된 제목을 그래프 함수에 전달 ---
        generate_plot(numeric_df.copy(), list(all_tickers), graph_title)
        print_final_report(numeric_df.copy(), list(all_tickers))


if __name__ == "__main__":
    main()
