import os
import sys
import time
import requests
import io
import zipfile
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. 환경 설정
# (GitHub Actions에서는 경로가 달라질 수 있으므로 절대 경로 처리)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")

engine = create_engine(DB_URI)
TABLE_NAME = "market_price_daily"


# 2. 종목 리스트 가져오기 (Tiingo 메뉴판)
def get_target_symbols():
    print("📥 Tiingo 전체 종목 리스트 다운로드 중...")
    try:
        url = "https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip"
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(z.open('supported_tickers.csv'))

        # 필터링: 미국(NYSE, NASDAQ) + 주식/ETF + 현재 상장중
        condition = (
                df['exchange'].isin(['NYSE', 'NASDAQ']) &
                df['assetType'].isin(['Stock', 'ETF']) &
                df['endDate'].isna()
        )
        df_clean = df[condition]

        # 상위 50개 + 주요 종목 강제 포함
        targets = df_clean['ticker'].head(50).tolist()
        majors = ['AAPL', 'TSLA', 'NVDA', 'QQQ', 'SPY', 'MSFT']
        for m in majors:
            if m not in targets:
                targets.insert(0, m)

        # 중복 제거 후 50개 맞추기
        return list(dict.fromkeys(targets))[:50]

    except Exception as e:
        print(f"⚠️ 리스트 다운로드 실패 ({e}), 기본 리스트 사용")
        return ['AAPL', 'QQQ', 'SPY', 'TSLA', 'NVDA']


# 3. DB 저장 함수
def save_to_db(df, conn):
    if df.empty: return
    df.to_sql('temp_daily_price', conn, if_exists='replace', index=False)
    query = f"""
    INSERT INTO {TABLE_NAME} (trade_date, open_price, high_price, low_price, close_price, volume, symbol)
    SELECT trade_date, open_price, high_price, low_price, close_price, volume, symbol
    FROM temp_daily_price
    ON CONFLICT (symbol, trade_date) DO UPDATE SET
        open_price = EXCLUDED.open_price,
        high_price = EXCLUDED.high_price,
        low_price = EXCLUDED.low_price,
        close_price = EXCLUDED.close_price,
        volume = EXCLUDED.volume;
    """
    conn.execute(text(query))
    conn.commit()


# 4. 메인 실행
def main():
    targets = get_target_symbols()
    print(f"🚀 총 {len(targets)}개 종목 수집 시작!")

    # 2024년 1월 1일부터 수집 (기간 조정 가능)
    start_date = "2024-01-01"
    headers = {'Content-Type': 'application/json'}

    with engine.connect() as conn:
        for i, ticker in enumerate(targets):
            try:
                print(f"[{i + 1}/{len(targets)}] {ticker}...", end=" ")
                url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices"
                params = {'startDate': start_date, 'token': TIINGO_API_KEY}

                res = requests.get(url, params=params, headers=headers)

                if res.status_code == 200:
                    data = res.json()
                    if data:
                        df = pd.DataFrame(data)
                        df = df.rename(columns={
                            'date': 'trade_date', 'adjOpen': 'open_price',
                            'adjHigh': 'high_price', 'adjLow': 'low_price',
                            'adjClose': 'close_price'
                        })
                        df['symbol'] = ticker
                        df = df[
                            ['trade_date', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'symbol']]
                        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.tz_localize(None)

                        save_to_db(df, conn)
                        print(f"✅ OK ({len(df)}일)")
                    else:
                        print("⚠️ No Data")
                else:
                    print(f"❌ Fail {res.status_code}")

                time.sleep(0.1)  # 속도 조절

            except Exception as e:
                print(f"Err: {e}")


if __name__ == "__main__":
    main()
