#!/usr/bin/env python
import sqlite3
import pandas as pd
import random
import time
import requests
import xml.etree.ElementTree as ET  # XML 파싱을 위한 내장 라이브러리
from typing import List, Tuple, Dict
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# --- 설정 (Configuration) ---
DB_FILE_PATH: str = "stock_price.db"
TABLE_NAME: str = "etf_dividends"
# POST 요청을 보낼 실제 API 주소
API_URL: str = "https://seibro.or.kr/websquare/engine/proworks/call_p.jsp"
USER_AGENT: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
REFERER_URL: str = "https://seibro.or.kr/websquare/websquare.jsp?w2xPath=/IPORTAL/user/etf/BIP_CNTS06030V.xml"

def random_delay(min_sec=0.5, max_sec=2.0):
    """지정된 시간 범위 내에서 랜덤한 대기 시간을 적용합니다."""
    time.sleep(random.uniform(min_sec, max_sec))


def create_etf_dividends_table(db_path: str) -> None:
    """etf_dividends 테이블을 생성합니다. (테이블이 없는 경우에만)"""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            ItemName TEXT,
            PaymentDate TEXT,
            Dividend REAL,
            PRIMARY KEY (ItemName, PaymentDate)
        );
        """
        cursor.execute(create_table_query)
        print(f"'{TABLE_NAME}' 테이블이 준비되었습니다.")


def generate_date_ranges(start_year: int) -> List[Tuple[date, date]]:
    """지정된 시작 연도부터 현재까지 3년 단위의 날짜 범위를 생성합니다."""
    start_date = date(start_year, 1, 1)
    end_date_limit = date.today()
    date_ranges = []
    current_start = start_date
    while current_start <= end_date_limit:
        current_end = current_start + relativedelta(years=3) - relativedelta(days=1)
        if current_end > end_date_limit:
            current_end = end_date_limit
        date_ranges.append((current_start, current_end))
        current_start += relativedelta(years=3)
    return date_ranges


def fetch_data_via_api(session: requests.Session, start_dt: date, end_dt: date) -> List[Dict]:
    """API를 직접 호출하여 주어진 기간 동안의 분배금 데이터를 가져옵니다."""
    print(f"API 요청 기간: {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')}")
    
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    # 서버가 요구하는 XML 형식의 요청 데이터를 생성합니다.
    # task와 action은 브라우저 개발자 도구에서 확인된 값을 사용합니다.
    payload = f'''
    <reqParam action="exerInfoDiv_pr" task="ksd.safe.bip.cnts.etf.process.EtfExerInfoPTask">
        <MENU_NO value="179"/>
        <CMM_BTN_ABBR_NM value="allview,search"/>
        <W2XPATH value="/IPORTAL/user/etf/BIP_CNTS06030V.xml"/>
        <in_SEARCH_CHECK value="1"/>
        <in_BEGIN_DATE value="{start_str}"/>
        <in_END_DATE value="{end_str}"/>
        <in_TYPE value="1"/>
        <in_ISIN value=""/>
        <in_KOR_SECN_NM value=""/>
        <in_APLI_CD value="1"/>
        <START_PAGE value="1"/>
        <END_PAGE value="9999"/>
    </reqParam>
    '''
    
    headers = {
        "Referer": REFERER_URL,
        "Content-Type": "application/xml;charset=UTF-8",
    }

    try:
        response = session.post(API_URL, headers=headers, data=payload.encode('utf-8'), timeout=30)
        response.raise_for_status()
        
        # XML 응답을 파싱합니다.
        root = ET.fromstring(response.content)
        api_result = []
        
        # XML 구조에 맞춰 데이터를 추출합니다.
        for data_node in root.findall(".//data"):
            result_node = data_node.find("result")
            if result_node is not None:
                item = {child.tag: child.get("value") for child in result_node}
                api_result.append(item)

        if not api_result:
            print(" -> 해당 기간에 데이터가 없습니다.")
            return []

        print(f" -> {len(api_result)} 건의 데이터를 수신했습니다.")
        return api_result
    
    except ET.ParseError:
        print(" -> 오류: 서버로부터 받은 XML 응답을 파싱할 수 없습니다.")
        print(f" -> 상태 코드: {response.status_code}")
        print(f" -> 응답 내용 (첫 500자): {response.text[:500]}...")
        return []
    except requests.exceptions.RequestException as e:
        print(f" -> 오류: API 요청 중 문제가 발생했습니다. ({e})")
        return []


def process_and_insert_data(db_path: str, data: List[Dict]) -> None:
    """수집한 데이터를 정제하고 DB에 저장합니다."""
    if not data:
        print("입력할 데이터가 없습니다.")
        return
        
    records = []
    for item in data:
        records.append({
            "ItemName": item.get("KOR_SECN_NM"),
            "PaymentDate": item.get("TH1_PAY_TERM_BEGIN_DT"),
            "Dividend": item.get("ESTM_STDPRC", "0").replace(",", ""),
        })

    df = pd.DataFrame(records)
    # 데이터 정제
    df.dropna(subset=["ItemName", "PaymentDate", "Dividend"], inplace=True)
    df = df[df["ItemName"] != ""]
    df["PaymentDate"] = pd.to_datetime(df["PaymentDate"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    df["Dividend"] = pd.to_numeric(df["Dividend"], errors="coerce")
    df.dropna(inplace=True)
    df = df[df["Dividend"] > 0]

    if df.empty:
        print(" -> 정제 후 입력할 유효한 데이터가 없습니다.")
        return

    insert_data = [tuple(x) for x in df[["ItemName", "PaymentDate", "Dividend"]].to_numpy()]

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        insert_query = f"INSERT OR IGNORE INTO {TABLE_NAME} (ItemName, PaymentDate, Dividend) VALUES (?, ?, ?)"
        cursor.executemany(insert_query, insert_data)
        conn.commit()
        changes = conn.total_changes if hasattr(conn, "total_changes") else cursor.rowcount
        print(f" -> 총 {len(insert_data)} 건 중 {changes} 건의 신규 정보를 DB에 입력했습니다.")


def main() -> None:
    """메인 실행 함수"""
    create_etf_dividends_table(DB_FILE_PATH)
    date_ranges = generate_date_ranges(start_year=2003)

    with requests.Session() as session:
        session.headers.update({"User-Agent": USER_AGENT})
        
        try:
            print("세션 초기화를 위해 메인 페이지에 접속합니다...")
            session.get(REFERER_URL, timeout=30)
            print("세션 초기화 완료.")
        except requests.exceptions.RequestException as e:
            print(f"오류: 초기 세션 생성에 실패했습니다. ({e})")
            return

        for start_dt, end_dt in date_ranges:
            period_data = fetch_data_via_api(session, start_dt, end_dt)
            if period_data:
                process_and_insert_data(DB_FILE_PATH, period_data)
            
            print("\n다음 기간 조회를 위해 대기 중...\n")
            random_delay(1.0, 4.0)

    print("\n모든 기간의 데이터 수집 및 저장이 완료되었습니다.")


if __name__ == "__main__":
    main()

