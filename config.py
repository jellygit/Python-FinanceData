# config.py

# 거래 비용 정의
BUY_COMMISSION_RATE = 0.0025  # 매수 시 수수료 0.25%
SELL_TAX_RATE = 0.0025  # 매도 시 세금/비용 0.25%

# 전략별 자산 목록 정의
STRATEGY_ASSETS = {
    "haa": {
        "offensive": ["SPY", "QQQ"],
        "defensive": ["IEF", "BIL"],
        "canary": ["SPY"],
    },
    "daa": {
        "offensive": [
            "SPY",
            "IWM",
            "QQQ",
            "VGK",
            "EWJ",
            "VWO",
            "VNQ",
            "GSG",
            "GLD",
            "TLT",
            "HYG",
            "LQD",
        ],
        "defensive": ["IEF", "LQD", "TLT"],
        "canary": ["SPY", "EEM", "EFA", "AGG"],
    },
    "laa": {
        "core": ["IWD", "GLD", "IEF"],
        "offensive": ["SPY"],
        "defensive": ["SHY"],
        "canary": ["SPY"],
    },
}
