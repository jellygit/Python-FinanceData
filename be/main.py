# main_api.py
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware  # 1. Middleware 임포트
from pydantic import BaseModel, Field
from typing import List, Optional
import backtest_engine


# --- API 요청 파라미터를 위한 Pydantic 모델 정의 ---
class BacktestParams(BaseModel):
    capital: float = Field(..., example=10000, description="초기 투자금")
    start_date: str = Field(
        ..., example="2020-01-01", description="시뮬레이션 시작일 (YYYY-MM-DD)"
    )
    end_date: Optional[str] = Field(
        None, example="2023-12-31", description="시뮬레이션 종료일"
    )
    db_path: str = Field("stock_price.db", description="DB 파일 경로")
    strategy: str = Field(
        "default", example="laa", description="투자 전략 (default, haa, daa, laa)"
    )
    interval: str = Field("1M", example="3M", description="리밸런싱 주기")
    periodic_investment: float = Field(
        0.0, example=1000, description="주기별 추가 투자금"
    )
    no_rebalance: bool = Field(
        False, description="[기본 전략용] 리밸런싱 없이 추가 매수만 진행"
    )
    stocks: Optional[List[str]] = Field(
        None,
        example=["SPY", "0.6", "AGG", "0.4"],
        description="[기본 전략용] 티커와 비중 목록",
    )
    rolling_window: Optional[int] = Field(
        None, example=3, description="롤링 리턴 기간 (단위: 연)"
    )
    rolling_step: str = Field("1Y", example="1Q", description="롤링 리턴 계산 빈도")


app = FastAPI()

# 2. CORS 미들웨어 추가
origins = [
    "*"
    # 필요하다면 실제 프론트엔드 서비스의 주소를 추가
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # 모든 메소드 허용
    allow_headers=["*"],  # 모든 헤더 허용
)


@app.get("/")
def read_root():
    return {"message": "Portfolio Backtest API"}


@app.post("/backtest")
def run_backtest_endpoint(params: BacktestParams):
    """
    백테스트 시뮬레이션을 실행하고 결과를 JSON으로 반환합니다.
    (주의: 실제 서비스에서는 이 부분을 Celery와 같은 Task Queue로 비동기 처리해야 합니다.)
    """
    try:
        # Pydantic 모델을 딕셔너리로 변환하여 백테스트 엔진에 전달
        params_dict = params.dict()
        results = backtest_engine.run_backtest(params_dict)
        return results
    except Exception as e:
        return {"error": str(e)}
