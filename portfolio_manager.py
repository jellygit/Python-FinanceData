# portfolio_manager.py

import pandas as pd
from config import BUY_COMMISSION_RATE, SELL_TAX_RATE


def _sweep_cash(holdings, cash, target_portfolio, prices):
    """[내부 함수] 잔여 현금을 목표 비중에 맞춰 추가 매수합니다."""
    target_prices = {
        t: prices.get(t)
        for t in target_portfolio
        if pd.notna(prices.get(t)) and prices.get(t) > 0
    }
    if target_prices and cash > min(target_prices.values()):
        print(f"--- 잔여 현금({cash:,.2f}) 추가 매수 실행 ---")
        cash_to_reinvest = cash - 1.0  # 거래 오류 방지용 버퍼

        for ticker, weight in target_portfolio.items():
            price = target_prices.get(ticker)
            if price is None:
                continue

            allocation = cash_to_reinvest * weight
            shares_to_buy = int(allocation / (price * (1 + BUY_COMMISSION_RATE)))

            if shares_to_buy > 0:
                base_cost = shares_to_buy * price
                commission = base_cost * BUY_COMMISSION_RATE
                total_cost = base_cost + commission
                if cash >= total_cost:
                    holdings[ticker] = holdings.get(ticker, 0) + shares_to_buy
                    cash -= total_cost
                    print(
                        f"- {ticker}: {shares_to_buy:,}주 잔여 현금 매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})"
                    )
    return holdings, cash


def execute_rebalancing(holdings, cash, target_portfolio, prices):
    """거래 비용 및 현금 최소화 로직을 포함하여 리밸런싱을 실행합니다."""
    current_portfolio_value = cash + sum(
        holdings.get(t, 0) * prices.get(t, 0)
        for t in holdings
        if pd.notna(prices.get(t))
    )

    tickers_to_sell = [
        t for t in holdings if holdings[t] > 0 and t not in target_portfolio
    ]
    for ticker in tickers_to_sell:
        shares = holdings[ticker]
        price = prices.get(ticker)
        if shares > 0 and pd.notna(price):
            holdings[ticker] = 0
            base_proceeds = shares * price
            tax = base_proceeds * SELL_TAX_RATE
            net_proceeds = base_proceeds - tax
            cash += net_proceeds
            print(
                f"- {ticker}: {shares:,}주 전량 매도 (금액: {base_proceeds:,.2f}, 비용: {tax:,.2f})"
            )

    for ticker, target_weight in target_portfolio.items():
        price = prices.get(ticker)
        if pd.isna(price) or price <= 0:
            continue

        target_value = current_portfolio_value * target_weight
        current_value = holdings.get(ticker, 0) * price
        delta_value = target_value - current_value

        if delta_value > 0:
            shares_to_buy = int(delta_value / (price * (1 + BUY_COMMISSION_RATE)))
            if shares_to_buy > 0:
                base_cost = shares_to_buy * price
                commission = base_cost * BUY_COMMISSION_RATE
                total_cost = base_cost + commission
                if cash >= total_cost:
                    holdings[ticker] = holdings.get(ticker, 0) + shares_to_buy
                    cash -= total_cost
                    print(
                        f"- {ticker}: {shares_to_buy:,}주 매수/추가매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})"
                    )
        elif delta_value < 0:
            shares_to_sell = int(-delta_value / price)
            if shares_to_sell > 0 and holdings.get(ticker, 0) >= shares_to_sell:
                holdings[ticker] -= shares_to_sell
                base_proceeds = shares_to_sell * price
                tax = base_proceeds * SELL_TAX_RATE
                net_proceeds = base_proceeds - tax
                cash += net_proceeds
                print(
                    f"- {ticker}: {shares_to_sell:,}주 비중조절 매도 (금액: {base_proceeds:,.2f}, 비용: {tax:,.2f})"
                )

    holdings, cash = _sweep_cash(holdings, cash, target_portfolio, prices)
    return holdings, cash


def execute_periodic_buy(holdings, cash, initial_target_weights, prices):
    """[수정] --no-rebalance 모드용. NaN 가격을 확인하여 오류를 방지합니다."""
    print(f"--- 주기적 추가 매수 (리밸런싱 없음) ---")

    cash_to_invest = cash

    for ticker, weight in initial_target_weights.items():
        price = prices.get(ticker)

        # [수정] pd.isna()를 사용하여 None과 NaN을 모두 확인
        if pd.isna(price) or price <= 0:
            continue

        allocation = cash_to_invest * weight
        shares_to_buy = int(allocation / (price * (1 + BUY_COMMISSION_RATE)))

        if shares_to_buy > 0:
            base_cost = shares_to_buy * price
            commission = base_cost * BUY_COMMISSION_RATE
            total_cost = base_cost + commission
            if cash >= total_cost:
                holdings[ticker] = holdings.get(ticker, 0) + shares_to_buy
                cash -= total_cost
                print(
                    f"- {ticker}: {shares_to_buy:,}주 추가 매수 (비용: {base_cost:,.2f}, 수수료: {commission:,.2f})"
                )

    holdings, cash = _sweep_cash(holdings, cash, initial_target_weights, prices)
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
        eval_result[f"{ticker} Weight"] = (
            stock_value / total_value if total_value > 0 else 0
        )

    return eval_result
