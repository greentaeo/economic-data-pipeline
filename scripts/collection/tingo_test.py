import os
import sys
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- 환경 설정 ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TABLE_NAME = "market_price_daily"


def test_single_stock():
    print(f"🔌 DB 연결 주소 확인: {DB_URI.split('@')[-1]}")
    engine = create_engine(DB_URI)

    # 테스트 대상: 애플(AAPL)
    ticker = "AAPL"
    print(f"\n🍏 [{ticker}] 데이터 수집 테스트 시작...")

    # 1. Tiingo 데이터 요청 (최근 1년치만)
    headers = {'Content-Type': 'application/json'}
    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
    params = {'startDate': '2024-01-01', 'token': TIINGO_API_KEY}

    res = requests.get(url, params=params, headers=headers)

    if res.status_code != 200:
        print(f"❌ API 호출 실패: {res.text}")
        return

    data = res.json()
    if not data:
        print("⚠️ 데이터가 비어있습니다.")
        return

    # 2. 데이터 가공
    df = pd.DataFrame(data)
    df = df.rename(columns={
        'date': 'trade_date',
        'adjOpen': 'open_price',  # 수정 주가 확인!
        'adjHigh': 'high_price',
        'adjLow': 'low_price',
        'adjClose': 'close_price'
    })
    df['symbol'] = ticker
    df = df[['trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'symbol']]

    # 날짜 포맷 정리
    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.tz_localize(None)

    print(f"📊 수신된 데이터: {len(df)}건")
    print(df.head(3))  # 눈으로 값 확인

    # 3. DB 저장 (Upsert)
    print("\n💾 Supabase DB에 저장 시도 중...")
    with engine.connect() as conn:
        df.to_sql('temp_test_stock', conn, if_exists='replace', index=False)

        upsert_query = f"""
        INSERT INTO {TABLE_NAME} (trade_date, open_price, high_price, low_price, close_price, volume, symbol)
        SELECT trade_date, open_price, high_price, low_price, close_price, volume, symbol
        FROM temp_test_stock
        ON CONFLICT (symbol, trade_date) DO UPDATE SET
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume;
        """
        conn.execute(text(upsert_query))
        conn.commit()

    print("✅ 저장 성공! 이제 TablePlus를 확인하세요.")


if __name__ == "__main__":
    test_single_stock()