import requests
import time
import logging
import pandas as pd
import sys
from datetime import datetime, timedelta
from pathlib import Path

# --- [설정 파일 연동] ---
# 프로젝트 루트 경로를 찾아 시스템 경로에 추가 (config 모듈 import용)
FILE = Path(__file__).resolve()
ROOT = FILE.parents[2]  # scripts/collection/ -> scripts/ -> root/
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config.settings import API_KEYS, DIRS, LOG_DIR

# --- [로깅 설정] ---
# 로그 파일도 이제 체계적으로 logs 폴더에 저장됩니다.
log_file = LOG_DIR / 'etf_smart_collector.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


class ETFSmartCollector:
    def __init__(self, years_back=5):
        # settings.py에서 API 키 가져오기
        self.api_key = API_KEYS['TIINGO']
        if not self.api_key:
            raise ValueError("TIINGO_API_KEY is missing in .env or settings!")

        self.base_url = "https://api.tiingo.com"
        self.years_back = years_back

        # settings.py에서 정의한 외장하드 경로 사용
        self.etf_path = DIRS['etf']

        # ETF 리스트 (기존 유지)
        self.etfs = {
            'SPY': 'S&P 500 ETF', 'QQQ': 'NASDAQ-100 ETF', 'DIA': 'Dow Jones ETF',
            'VTI': 'Total Stock Market ETF', 'IWM': 'Russell 2000 ETF', 'XLF': 'Financial Sector ETF',
            'XLK': 'Technology Sector ETF', 'XLE': 'Energy Sector ETF', 'XLV': 'Healthcare Sector ETF',
            'XLRE': 'Real Estate Sector ETF', 'EFA': 'Developed Markets ETF', 'EEM': 'Emerging Markets ETF',
            'VEA': 'Developed Markets Ex-US ETF', 'TLT': '20+ Year Treasury ETF', 'IEF': '7-10 Year Treasury ETF',
            'LQD': 'Investment Grade Corporate Bond ETF', 'HYG': 'High Yield Corporate Bond ETF',
            'GLD': 'Gold ETF', 'SLV': 'Silver ETF', 'USO': 'Oil ETF', 'DBA': 'Agriculture ETF'
        }
        logging.info(f"ETF Collector initialized. Target Dir: {self.etf_path}")

    def get_etf_data(self, symbol, start_date, end_date):
        """Tiingo API를 통해 특정 기간의 ETF 데이터를 가져옵니다."""
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        url = f"{self.base_url}/tiingo/daily/{symbol}/prices"
        params = {'token': self.api_key, 'startDate': start_str, 'endDate': end_str}

        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if not data: return None  # 데이터가 빈 리스트일 경우 처리

                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                logging.info(f"✅ {symbol}: Fetched {len(df)} rows.")
                return df[['adjClose']].rename(columns={'adjClose': 'Adj Close'})
            elif response.status_code == 429:
                logging.warning("Rate limit reached! Waiting for 60 seconds...")
                time.sleep(60)
            else:
                logging.error(f"{symbol}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"{symbol}: Error: {e}")
        return None

    def collect_and_save_etf(self, symbol):
        filename = self.etf_path / f"{symbol}.csv"
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.years_back * 365)

        existing_data = None
        if filename.exists():
            try:
                existing_data = pd.read_csv(filename, index_col=0, parse_dates=True)
                latest_date = existing_data.index.max()
                if latest_date < end_date - timedelta(days=1):
                    start_date = latest_date + timedelta(days=1)
                else:
                    logging.info(f"⏭️ {symbol}: Already up-to-date.")
                    return True
            except Exception as e:
                logging.warning(f"File read error {symbol}: {e}. Fetching all.")

        new_data = self.get_etf_data(symbol, start_date, end_date)

        if new_data is not None and not new_data.empty:
            if existing_data is not None:
                combined_data = pd.concat([existing_data, new_data])
                combined_data = combined_data[~combined_data.index.duplicated(keep='first')].sort_index()
            else:
                combined_data = new_data

            combined_data.to_csv(filename)
            logging.info(f"💾 {symbol}: Saved to {filename}")
            return True
        return False

    def run_collection(self):
        logging.info("=== Starting ETF Collection ===")
        for i, symbol in enumerate(self.etfs.keys()):
            self.collect_and_save_etf(symbol)
            if i < len(self.etfs) - 1:
                time.sleep(1)  # Tiingo 무료 티어 제한 고려 (약간 빠르게 조정)
        logging.info("=== Finished ===")


if __name__ == "__main__":
    ETFSmartCollector(years_back=5).run_collection()