#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
포트폴리오 리밸런싱 시뮬레이션 스크립트
"""

import argparse
import sqlite3
import sys
from datetime import datetime

import pandas as pd


def load_data(db_path, tickers):
    """SQLite 데이터베이스에서 주가 데이터를 불러옵니다."""
    try:
        with sqlite3.connect(db_path) as con:
            placeholders = ", ".join("?" for _ in tickers)
            query = (
                f"SELECT Date, Symbol, Close FROM stock_price "
                f"WHERE Symbol IN ({placeholders}) ORDER BY Date"
            )
            df = pd.read_sql_query(query, con, params=tickers)
            df.rename(
                columns={"Date": "date", "Symbol": "ticker", "Close": "close"},
                inplace=True,
            )
            df["date"] = pd.to_datetime(df["date"])
            pivot_df = df.pivot(index="date", columns="ticker", values="close")
            pivot_df.ffill(inplace=True)
            return pivot_df
    except sqlite3.Error as e:
        sys.exit(f"데이터베이스 오류: {e}")
    except Exception as e:
        sys.exit(f"데이터 로딩 중 오류 발생: {e}")


def evaluate_portfolio(
    holdings,
    cash,
    eval_date,
    stock_data,
    tickers,
    target_weights,
    rebalance=True,
):
    """특정 날짜를 기준으로 포트폴리오를 평가하고 리밸런싱합니다."""
    try:
        current_prices = stock_data.loc[eval_date]
    except KeyError:
        return holdings, cash, None

    total_value = cash
    eval_result = {"Date": eval_date.strftime("%Y-%m-%d"), "Cash": cash}

    for ticker in tickers:
        stock_value = holdings[ticker] * current_prices[ticker]
        total_value += stock_value
        eval_result[f"{ticker} Holdings"] = holdings[ticker]
        eval_result[f"{ticker} Price"] = current_prices[ticker]
        eval_result[f"{ticker} Value"] = stock_value

    if total_value > 0:
        for ticker in tickers:
            weight = (holdings[ticker] * current_prices[ticker]) / total_value
            eval_result[f"{ticker} Weight"] = weight

    if rebalance:
        print(f"\n--- 리밸런싱 실행: {eval_date.strftime('%Y-%m-%d')} ---")
        for ticker in tickers:
            current_value = holdings[ticker] * current_prices[ticker]
            target_value = total_value * target_weights[ticker]
            delta_value = target_value - current_value
            price = current_prices[ticker]

            if price <= 0:
                continue

            if delta_value > 0:
                shares_to_buy = int(delta_value / price)
                cost = shares_to_buy * price
                if cash >= cost:
                    holdings[ticker] += shares_to_buy
                    cash -= cost
                    print(
                        f"- {ticker}: {shares_to_buy:,}주 매수 (주당 {price:.2f}, 총 {cost:,.2f})"
                    )
            elif delta_value < 0:
                shares_to_sell = int(-delta_value / price)
                if holdings[ticker] >= shares_to_sell:
                    holdings[ticker] -= shares_to_sell
                    proceeds = shares_to_sell * price
                    cash += proceeds
                    print(
                        f"- {ticker}: {shares_to_sell:,}주 매도 (주당 {price:.2f}, 총 {proceeds:,.2f})"
                    )
        print(f"리밸런싱 후 현금: {cash:,.2f}")

    return holdings, cash, eval_result


def main():
    """포트폴리오 시뮬레이션을 실행하는 메인 함수"""
    parser = argparse.ArgumentParser(description="포트폴리오 리밸런싱 시뮬레이션")
    parser.add_argument("capital", type=float, help="초기 투자금")
    parser.add_argument(
        "stocks", nargs="+", help="티커와 비중 목록 (예: AAPL 0.25 MSFT 0.25)"
    )
    parser.add_argument(
        "--start-date", required=True, help="시뮬레이션 시작일 (YYYY-MM-DD)"
    )
    parser.add_argument("--interval", default="3M", help="리밸런싱 주기 (예: 3M, 1Y)")
    parser.add_argument(
        "--db-path", required=True, help="SQLite 데이터베이스 파일 경로"
    )
    args = parser.parse_args()

    if len(args.stocks) % 2 != 0:
        sys.exit("티커와 비중은 반드시 쌍으로 입력해야 합니다.")

    tickers = args.stocks[::2]
    try:
        weights = [float(w) for w in args.stocks[1::2]]
    except ValueError:
        sys.exit("비중은 반드시 숫자여야 합니다.")

    if round(sum(weights), 5) != 1.0:
        sys.exit(f"비중의 합은 1.0이어야 합니다. 현재 합: {sum(weights)}")

    target_weights = dict(zip(tickers, weights))
    print("데이터를 로딩합니다...")
    stock_data = load_data(args.db_path, tickers)

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        start_date = stock_data.index[stock_data.index.searchsorted(start_date)]
    except IndexError:
        sys.exit(f"시작일({args.start_date}) 이후의 데이터가 없습니다.")

    print("\n--- 초기 투자 시뮬레이션 ---")
    print(f"매수 기준일: {start_date.strftime('%Y-%m-%d')}")
    initial_prices = stock_data.loc[start_date]

    if initial_prices.isnull().any():
        missing_tickers = initial_prices[initial_prices.isnull()].index.tolist()
        sys.exit(
            f"\n오류: 매수 기준일({start_date.strftime('%Y-%m-%d')})에 다음 티커의 "
            f"가격 데이터가 없습니다: {', '.join(missing_tickers)}\n"
            "--> 데이터가 존재하는 더 늦은 날짜를 시작일로 지정해 주세요."
        )

    cash = args.capital
    holdings = {ticker: 0 for ticker in tickers}

    for ticker in tickers:
        price = initial_prices[ticker]
        if price <= 0:
            continue
        allocation = args.capital * target_weights[ticker]
        num_shares = int(allocation / price)
        cost = num_shares * price
        holdings[ticker] = num_shares
        cash -= cost
        print(f"- {ticker}: {num_shares:,}주 매수 (주당 {price:.2f}, 총 {cost:,.2f})")
    print(f"초기 매수 후 남은 현금: {cash:,.2f}")

    results = []
    initial_snapshot = {"Date": start_date.strftime("%Y-%m-%d"), "Cash": cash}
    total_value = cash

    for ticker in tickers:
        stock_value = holdings[ticker] * initial_prices[ticker]
        total_value += stock_value
        initial_snapshot[f"{ticker} Holdings"] = holdings[ticker]
        initial_snapshot[f"{ticker} Price"] = initial_prices[ticker]
        initial_snapshot[f"{ticker} Value"] = stock_value

    for ticker in tickers:
        stock_value = initial_snapshot[f"{ticker} Value"]
        weight = stock_value / total_value if total_value > 0 else 0
        initial_snapshot[f"{ticker} Weight"] = weight

    results.append(initial_snapshot)

    evaluation_dates_theory = pd.date_range(
        start=start_date, end=stock_data.index[-1], freq=args.interval
    )
    evaluation_dates = evaluation_dates_theory[1:]

    for eval_date in evaluation_dates:
        try:
            actual_eval_date = stock_data.index[
                stock_data.index.searchsorted(eval_date)
            ]
            holdings, cash, eval_result = evaluate_portfolio(
                holdings.copy(),
                cash,
                actual_eval_date,
                stock_data,
                tickers,
                target_weights,
            )
            if eval_result:
                results.append(eval_result)
        except IndexError:
            break

    if results:
        result_df = pd.DataFrame(results)
        result_df.set_index("Date", inplace=True)
        value_columns = [f"{ticker} Value" for ticker in tickers]
        result_df["Portfolio Value"] = (
            result_df[value_columns].sum(axis=1) + result_df["Cash"]
        )

        for ticker in tickers:
            result_df[f"{ticker} Weight"] = result_df[f"{ticker} Weight"].apply(
                lambda x: f"{x:.2%}" if pd.notna(x) else "N/A"
            )
            result_df[f"{ticker} Value"] = result_df[f"{ticker} Value"].apply(
                lambda x: f"{x:,.2f}"
            )

        result_df["Portfolio Value"] = result_df["Portfolio Value"].apply(
            lambda x: f"{x:,.2f}"
        )
        result_df["Cash"] = result_df["Cash"].apply(lambda x: f"{x:,.2f}")

        display_columns = ["Portfolio Value", "Cash"]
        for ticker in tickers:
            display_columns.extend(
                [
                    f"{ticker} Holdings",
                    f"{ticker} Price",
                    f"{ticker} Value",
                    f"{ticker} Weight",
                ]
            )

        display_columns = [col for col in display_columns if col in result_df.columns]

        with pd.option_context(
            "display.max_rows", None, "display.max_columns", None, "display.width", 1000
        ):
            print("\n--- 포트폴리오 가치 평가 결과 ---")
            print(result_df[display_columns])


if __name__ == "__main__":
    main()
