import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pandas_datareader import fred
from datetime import datetime

# 님의 지표 목록이 있는 파일에서 리스트를 가져옵니다.
from indicators import fred_indicators

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
if not DB_URI:
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

TABLE_NAME = "macro_time_series"


def get_last_date_from_db(conn, indicator_symbol):
    """DB에서 특정 지표의 마지막 날짜를 조회합니다."""
    # DB에 데이터가 있을 경우, 마지막 날짜 다음 날부터 수집 시작
    query = text(f"""
        SELECT MAX(date_time) FROM {TABLE_NAME} 
        WHERE indicator_symbol = :symbol
    """)
    result = conn.execute(query, {'symbol': indicator_symbol}).scalar()

    if result:
        # 마지막 날짜의 '다음 날'부터 수집 시작
        return result.strftime('%Y-%m-%d')
    # DB에 데이터가 없으면 1년 전부터 시작
    return (datetime.now() - pd.Timedelta(days=365)).strftime('%Y-%m-%d')


def collect_fred_data():
    print("🚀 FRED 경제 지표 자동 업데이트 시작...")
    engine = create_engine(DB_URI)

    # 님께서 정의한 모든 지표 ID를 하나의 리스트로 만듭니다.
    all_symbols = [d['id'] for category in fred_indicators.values() for d in category]

    with engine.connect() as conn:
        for symbol in all_symbols:

            # DB에서 마지막 업데이트 날짜를 가져옵니다.
            last_date = get_last_date_from_db(conn, symbol)

            # FRED에서 수집 시작!
            try:
                # pandas_datareader를 사용해 FRED에서 데이터 요청
                # start=last_date로 설정하여 님의 유료 데이터 '다음 날'부터 가져옵니다.
                df = fred.FredReader(symbols=symbol, start=last_date, end=datetime.now()).read()

                # 데이터가 없는 경우
                if df.empty or len(df) <= 1:
                    print(f"   ⚠️ {symbol}: 새로운 데이터 없음.")
                    continue

                # 데이터 정리
                df = df.reset_index()
                df.columns = ['date_time', 'value']
                df['indicator_symbol'] = symbol
                df['country'] = "United States"

                # DB에 이어 붙이기 (append)
                df[['date_time', 'indicator_symbol', 'value', 'country']].to_sql(
                    TABLE_NAME, conn, if_exists='append', index=False
                )

                print(f"   ✅ {symbol}: {len(df)}개 신규 데이터 업데이트 완료.")

            except Exception as e:
                print(f"   ❌ {symbol} 수집 에러: {e}")

        conn.commit()  # 커밋

    print("🎉 FRED 경제 지표 자동 업데이트 완료!")


if __name__ == "__main__":
    collect_fred_data()
