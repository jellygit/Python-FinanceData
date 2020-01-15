# Finance Data
## init_data.py
사용 방법
> python ./init_data.py
1. https://github.com/FinanceData/FinanceDataReader 모듈 사용
1. 한국과 미국 종목코드 받아오기.
1. 한국 1996년 부터 2019년 12월 30일까지 전종목 시가-종가-거래량 저장
 1. ./db/finance.db 로 저장되며, 650 MB 가량 됨.
 1. 종목 코드는 DB에 덮어씌우지만, 거래 데이터는 이어쓰기(Append)이기 때문에 중복 발생할 수 있음. 중복 발생시엔 db_sort.py 로 정리 할 것.

## stock_value.py
사용 방법
> python ./stock_value.py NNNNNN
1. NNNNNN은 종목 코드, db/finance.db 에서 거래 데이터 받아와 매달 첫 거래일 종가에 매수 했을 때의 데이터를 출력함.
1. 표출출력하게 되어 있음.
1. 2020 년 까지 실행 시 에러가 남. -> 수정 완료, 날짜 중복 시 배열이 넘어와서 에러 발생한 것.
 1. 최신 거래 데이터 지속적으로 갱신하다 보면 중복발생할 수 있음. db_sort.py 사용

## db/finance.db.zst
사용 방법
> zstd -d db/finance.db.zst
1. Z Standard 로 압축 되어 있음, zstd -d  옵션으로 압축해제 후 사용
### Describe (schema)
db/finance.db 파일 테이블.
#### KRX: 한국주식 종목 코드
```sql
CREATE TABLE IF NOT EXISTS "KRX" (
"index" INTEGER,
  "Symbol" TEXT,
  "Name" TEXT,
  "Sector" TEXT,
  "Industry" TEXT
);
```
#### 개별종목
```sql
CREATE TABLE IF NOT EXISTS "종목코드" (
"Date" TIMESTAMP,
  "Open" INTEGER,
  "High" INTEGER,
  "Low" INTEGER,
  "Close" INTEGER,
  "Volume" INTEGER,
  "Change" REAL
);
```


## db_sort.py
db/finance.db 파일에 중복된 거래 데이터가 생겼을 때, 날짜기준으로 중복 제거하는 스크립트
> python ./db_sort.py
* 그냥 실행하면 모든 종목코드 돌면서 중복 날짜 삭제.
