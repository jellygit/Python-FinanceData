# reporting.py

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from config import TICKER_NAMES


def generate_plot(plot_df, all_tickers, title):
    """[수정] 동적 제목과 종목명 범례를 사용하여 그래프를 생성합니다."""
    plot_df.index = pd.to_datetime(plot_df.index)
    fig, ax1 = plt.subplots(figsize=(18, 9))

    # 왼쪽 Y축
    ax1.set_yscale("log")
    ax1.plot(
        plot_df.index,
        plot_df["Portfolio Value"],
        label="Portfolio Value",
        color="royalblue",
        linewidth=2.5,
    )
    ax1.plot(
        plot_df.index,
        plot_df["Total Investment"],
        label="Total Investment",
        color="red",
        linestyle="--",
        linewidth=2,
    )
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Amount (Log Scale)")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f"{int(x):,}"))
    ax1.yaxis.set_minor_formatter(mticker.FuncFormatter(lambda x, p: f"{int(x):,}"))
    ax1.grid(True, which="both", ls="--", linewidth=0.5)

    # 오른쪽 Y축
    ax2 = ax1.twinx()

    # 자산 비중 배경 그래프 (범례는 여기서 생성)
    weight_columns = [f"{ticker} Weight" for ticker in all_tickers]
    # [수정] 범례 레이블을 '티커 종목명' 형식으로 생성
    legend_labels = [
        f"{ticker} {TICKER_NAMES.get(ticker, '')}".strip() for ticker in all_tickers
    ]
    ax2.stackplot(
        plot_df.index, plot_df[weight_columns].T, labels=legend_labels, alpha=0.3
    )

    # ROI 라인 그래프
    roi_data = plot_df["ROI"].fillna(0)
    ax2.fill_between(
        plot_df.index,
        roi_data,
        0,
        where=(roi_data < 0),
        color="darkorange",
        alpha=0.3,
        interpolate=True,
        label="ROI Loss (Below 0%)",
    )
    ax2.plot(
        plot_df.index,
        plot_df["ROI"],
        label="ROI",
        color="green",
        linestyle="-",
        marker="o",
        markersize=4,
        linewidth=2,
    )
    ax2.axhline(0, color="grey", linestyle=":", linewidth=1)

    ax2.set_ylabel("Return on Investment (ROI) (%)")
    ax2.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))

    # 그래프 설정
    plt.title(title, fontsize=16)  # [수정] 동적 제목 사용

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()

    # 범례 핸들과 레이블을 합치되, 중복 제거
    # stackplot의 범례는 이미 labels2에 포함되어 있음
    unique_labels = {}
    for item in list(zip(lines1, labels1)) + list(zip(lines2, labels2)):
        if item[1] not in unique_labels:
            unique_labels[item[1]] = item[0]

    ax1.legend(
        unique_labels.values(),
        unique_labels.keys(),
        loc="upper left",
        bbox_to_anchor=(0.05, 0.95),
    )

    plt.setp(ax1.get_yticklabels(minor=True), fontsize="small")
    fig.tight_layout()

    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".webp"
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    print(f"\n📈 그래프를 '{filename}' 파일로 저장했습니다.")


def print_final_report(numeric_df, all_tickers):
    """ROI를 포함하여 최종 결과를 데이터프레임으로 만들어 화면에 출력합니다."""
    if numeric_df.empty:
        print("시뮬레이션 결과가 없습니다.")
        return

    display_df = numeric_df.copy()

    for ticker in all_tickers:
        col_name = f"{ticker} Weight"
        if col_name in display_df.columns:
            display_df[col_name] = display_df[col_name].apply(lambda x: f"{x:.2%}")
    if "ROI" in display_df.columns:
        display_df["ROI"] = display_df["ROI"].apply(lambda x: f"{x:.2%}")

    for col in display_df.columns:
        if "Value" in col or "Price" in col or "Cash" in col or "Investment" in col:
            display_df[col] = display_df[col].apply(lambda x: f"{x:,.2f}")

    display_columns = ["Portfolio Value", "Total Investment", "ROI", "Cash"]
    for ticker in all_tickers:
        display_columns.extend(
            [
                f"{ticker} Holdings",
                f"{ticker} Price",
                f"{ticker} Value",
                f"{ticker} Weight",
            ]
        )

    display_columns = [col for col in display_columns if col in display_df.columns]

    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 200
    ):
        print("\n--- 포트폴리오 가치 평가 결과 ---")
        print(display_df[display_columns])
