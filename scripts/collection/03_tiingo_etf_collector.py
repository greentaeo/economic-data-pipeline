import os
import sys
import requests  # <-- requests 라이브러리 사용 (직접 통신)
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

# --- 환경 변수 설정 ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")

if not DB_URI:
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

TABLE_NAME = "market_price_daily"


# --- [DB 저장 함수: UPSERT] ---
def save_data(df: pd.DataFrame, conn, table_name):
    """
    DB에 데이터를 UPSERT (UPDATE OR INSERT) 방식으로 저장합니다.
    """
    if df.empty:
        return

    # 임시 테이블로 먼저 저장합니다.
    df.to_sql('temp_tiingo_data', conn, if_exists='replace', index=False)

    # 임시 테이블의 데이터를 최종 테이블로 UPSERT 합니다.
    # Tiingo API의 원본 컬럼명(date, open, high...)을 우리 DB 컬럼명으로 매핑하여 넣습니다.
    upsert_query = f"""
    INSERT INTO {table_name} (trade_date, open_price, high_price, low_price, close_price, volume, symbol)
    SELECT 
        trade_date, 
        open_price, 
        high_price, 
        low_price, 
        close_price, 
        volume, 
        symbol
    FROM temp_tiingo_data
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume;
    """
    conn.execute(text(upsert_query))
    conn.commit()


def get_last_date(conn, symbol):
    """DB에서 특정 심볼의 마지막 날짜를 조회합니다."""
    query = text(f"SELECT MAX(trade_date) FROM {TABLE_NAME} WHERE symbol = :symbol")
    result = conn.execute(query, {'symbol': symbol}).scalar()

    if result:
        return (pd.to_datetime(result) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    # 데이터가 없으면 10년 전부터 시작
    return (datetime.now() - pd.Timedelta(days=365 * 10)).strftime('%Y-%m-%d')


# --- [메인 수집 로직: Requests 사용] ---
def collect_etf_data():
    if not TIINGO_API_KEY:
        print("❌ ERROR: TIINGO_API_KEY가 없습니다.")
        return

    print("🚀 ETF 데이터 수집 시작 (Tiingo Direct API)...")
    engine = create_engine(DB_URI)

    TICKERS = ["QQQ", "SPY", "GLD", "TLT"]

    # HTTP 헤더 설정
    headers = {
        'Content-Type': 'application/json'
    }

    with engine.connect() as conn:
        for ticker in TICKERS:
            # 1. 시작 날짜 계산
            start_date = get_last_date(conn, ticker)
            print(f"   🔄 {ticker}: {start_date} 부터 데이터 요청 중...")

            try:
                # 2. Tiingo REST API 직접 호출
                url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
                params = {
                    'startDate': start_date,
                    'token': TIINGO_API_KEY
                }

                response = requests.get(url, params=params, headers=headers)

                if response.status_code != 200:
                    print(f"   ⚠️ {ticker} API 호출 실패: {response.text}")
                    continue

                data = response.json()

                if not data:
                    print(f"   ⚠️ {ticker}: 새로운 데이터 없음.")
                    continue

                # 3. JSON 데이터를 DataFrame으로 변환
                df = pd.DataFrame(data)

                # 4. 컬럼 이름 매핑 (Tiingo API -> 우리 DB 구조)
                # Tiingo는 date, open, high, low, close, volume, adjClose... 등을 줍니다.
                df = df.rename(columns={
                    'date': 'trade_date',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                    'close': 'close_price',
                    # volume은 그대로 volume
                })

                # 필요한 컬럼만 남기기
                df['symbol'] = ticker
                df = df[['trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'symbol']]

                # 날짜 형식 정리 (ISO 포맷 -> datetime)
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.tz_localize(None)

                # 5. DB 저장
                save_data(df, conn, TABLE_NAME)

                print(f"   ✅ {ticker}: {len(df)}개 데이터 저장 완료.")

            except Exception as e:
                print(f"   ❌ {ticker} 에러 발생: {e}")

    print("🎉 모든 ETF 데이터 업데이트 완료!")


if __name__ == "__main__":
    collect_etf_data()