import requests
import time
import logging
import pandas as pd
import sys
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine, text

# --- [설정 파일 연동] ---
FILE = Path(__file__).resolve()
ROOT = FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config.settings import API_KEYS, LOG_DIR

# --- [DB 접속 정보] ---
# 실전에서는 settings.py에 넣지만, 일단 연습이니까 여기에 둡니다.
DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

# --- [로깅 설정] ---
log_file = LOG_DIR / 'etf_db_collector.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


class SmartETFCollector:
    def __init__(self, years_back=1):
        self.api_key = API_KEYS['TIINGO']
        self.base_url = 'https://api.tiingo.com'
        self.years_back = years_back
        self.years_back = years_back
        self.engine = create_engine(DB_URI)
        self.etfs = ['SPY', 'QQQ', 'GLD']
        logging.info("🧠 Smart ETF Collector initialized.")

    def get_last_date_from_db(self, ticker):
        """DB에 접속해서 해당 종목의 '가장 마지막 날짜'를 알아옵니다."""
        try:
            with self.engine.connect() as conn:
            # 쿼리: ticker가 일치하는 데이터 중 가장 큰(MAX) 날짜를 가져와라
                query = text("SELECT MAX(trade_date) FROM practice_spy WHERE ticker = :ticker")
                result = conn.execute(query, {'ticker': ticker}).fetchone()

                if result and result[0]:
                    return result[0]
        except Exception as e:
            logging.warning(f"⚠️ {ticker} 날짜 조회 실패 (첫 수집으로 간주): {e}")
        return None

    def get_etf_data(self, symbol, start_date, end_date):
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        logging.info(f"REQUEST: {symbol} ({start_str} ~ {end_str})")

        url = f"{self.base_url}/tiingo/daily/{symbol}/prices"
        params = {'token': self.api_key, 'startDate': start_str, 'endDate': end_str}

        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if not data: return None

                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])

                # DB 테이블 컬럼명에 맞게 데이터 정리
                # practice_spy 구조: ticker, trade_date, close_price
                df = df[['date', 'adjClose']].copy()
                df.columns = ['trade_date', 'close_price']  # 이름 변경
                df['ticker'] = symbol  # ticker 컬럼 추가
                251#1*
                # 컬럼 순서 맞추기 (보기 좋게)
                df = df[['ticker', 'trade_date', 'close_price']]

                logging.info(f"✅ {symbol}: Fetched {len(df)} rows.")
                return df
            else:
                logging.error(f"{symbol}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"{symbol}: Error: {e}")
        return None

    def save_to_db(self, df):
        if df is None or df.empty:
            return

        try:
            # 🚀 여기가 핵심! to_sql로 DB에 바로 쏘기
            # if_exists='append': 데이터가 있으면 그 뒤에 이어 붙여라
            # index=False: 판다스 숫자 인덱스(0,1,2...)는 넣지 마라
            df.to_sql('practice_spy', self.engine, if_exists='append', index=False)
            logging.info(f"💾 Saved {len(df)} rows to DB (practice_spy)")
        except Exception as e:
            logging.error(f"❌ DB Save Failed: {e}")

    def run(self):
        today = datetime.now()

        for symbol in self.etfs:
            logging.info(f"--- Checking {symbol} ---")

            # 1. DB에서 마지막 날짜 확인 (이어달리기)
            last_db_date = self.get_last_date_from_db(symbol)

            if last_db_date:
                # 마지막 날짜가 있으면, 그 '다음 날'부터 수집 시작
                # last_db_date는 date 타입이므로 datetime으로 변환 필요할 수 있음
                if isinstance(last_db_date, str):
                    last_db_date = datetime.strptime(last_db_date, '%Y-%m-%d').date()

                start_date = last_db_date + timedelta(days=1)
                # start_date를 datetime 객체로 변환 (API 함수 호환성 위함)
                start_date = datetime(start_date.year, start_date.month, start_date.day)

                logging.info(f"🔄 이어달리기: DB 마지막 날짜는 {last_db_date}. {start_date.date()}부터 수집합니다.")
            else:
                # DB에 데이터가 없으면 설정된 기간만큼 수집
                start_date = today - timedelta(days=self.years_back * 365)
                logging.info(f"🆕 신규 수집: {start_date.date()}부터 시작합니다.")
                # 2. 이미 최신이면 건너뛰기
            if start_date < today:
                logging.info(f"✅ {symbol}은 이미 최신 데이터입니다. 건너뜁니다.")
                continue

                # 3. 데이터 수집 및 저장
            df = self.get_etf_date(symbol, start_date, today)
            if df is not None and not df.empty:
                self.save_to_db(df)
            else:
                logging.info(f"🤷‍♂️ {symbol}: 가져올 새로운 데이터가 없습니다.")

            time.sleep(1)

if __name__ == "__main__":
    SmartETFCollector(years_back=1).run()