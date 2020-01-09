# Finance Data
## init_data.py
사용 방법
> python ./init_data.py
1. https://github.com/FinanceData/FinanceDataReader 모듈 사용
1. 한국과 미국 종목코드 받아오기.
1. 한국 1996년 부터 2019년 12월 30일까지 전종목 시가-종가-거래량 저장
 1. ./db/finance.db 로 저장되며, 650 MB 가량 됨.

## stock_value.py
사용 방법
> python ./stock_value.py NNNNNN
1. NNNNNN은 종목 코드, db/finance.db 에서 거래 데이터 받아와 매달 첫 거래일 종가에 매수 했을 때의 데이터를 출력함.
1. 표출출력하게 되어 있음.
1. 2020 년 까지 실행 시 에러가 남.

## db/finance.db.zst
사용 방법
> zst -d db/finance.db.zst
1. Z Standard 로 압축 되어 있음, zst -d  옵션으로 압축해제 후 사용
