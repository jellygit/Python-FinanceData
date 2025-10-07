# reporting.py

from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


def generate_plot(plot_df, all_tickers):
    """자산 비중과 ROI를 동시에 표시하며, ROI 손실 구간을 강조하는 그래프를 생성합니다."""
    plot_df.index = pd.to_datetime(plot_df.index)
    fig, ax1 = plt.subplots(figsize=(18, 9))

    # 왼쪽 Y축 (금액, 로그 스케일)
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

    # 자산 비중을 반투명한 배경 그래프로 먼저 그립니다.
    weight_columns = [f"{ticker} Weight" for ticker in all_tickers]
    ax2.stackplot(
        plot_df.index, plot_df[weight_columns].T, labels=weight_columns, alpha=0.3
    )

    # ROI 선 그래프가 0% 이하일 때 면을 강조 표시
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

    # ROI 라인 그래프를 선명하게 겹쳐 그립니다.
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

    # 0% 기준선 표시
    ax2.axhline(0, color="grey", linestyle=":", linewidth=1)

    # 오른쪽 축의 이름과 눈금은 ROI를 기준으로 설정합니다.
    ax2.set_ylabel("Return on Investment (ROI) (%)")
    ax2.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))

    # 그래프 설정
    plt.title("Portfolio Value, Asset Allocation, and ROI Over Time", fontsize=16)

    # [수정] 양쪽 축의 범례를 올바른 방식으로 합쳐서 표시합니다.
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()

    combined_items = list(zip(lines1, labels1)) + list(zip(lines2, labels2))

    # 범례 레이블의 중복을 제거하여 깔끔하게 표시
    unique_labels = {}
    for line, label in combined_items:
        if label not in unique_labels:
            unique_labels[label] = line

    ax1.legend(
        unique_labels.values(),
        unique_labels.keys(),
        loc="upper left",
        bbox_to_anchor=(0.05, 0.95),
    )

    plt.setp(ax1.get_yticklabels(minor=True), fontsize="small")
    fig.tight_layout()

    # 파일 저장
    filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".webp"
    plt.savefig(filename, dpi=150, bbox_inches="tight")
    print(
        f"\n📈 그래프를 '{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.webp' 파일로 저장했습니다."
    )


def print_final_report(numeric_df, all_tickers):
    """ROI를 포함하여 최종 결과를 데이터프레임으로 만들어 화면에 출력합니다."""
    if numeric_df.empty:
        print("시뮬레이션 결과가 없습니다.")
        return

    display_df = numeric_df.copy()

    # Weight 및 ROI 포맷팅
    for ticker in all_tickers:
        col_name = f"{ticker} Weight"
        if col_name in display_df.columns:
            display_df[col_name] = display_df[col_name].apply(lambda x: f"{x:.2%}")
    if "ROI" in display_df.columns:
        display_df["ROI"] = display_df["ROI"].apply(lambda x: f"{x:.2%}")

    # 금액 및 가격 포맷팅
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
