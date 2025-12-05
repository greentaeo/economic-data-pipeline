import requests
import time
import logging
import pandas as pd
import sys
from datetime import datetime, timedelta
from pathlib import Path

# --- [설정 파일 연동] ---
FILE = Path(__file__).resolve()
ROOT = FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config.settings import API_KEYS, DIRS, LOG_DIR

# --- [로깅 설정] ---
log_file = LOG_DIR / 'forex_simple_collector.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


class ForexSimpleCollector:
    def __init__(self):
        self.api_key = API_KEYS['TIINGO']
        if not self.api_key:
            raise ValueError("TIINGO_API_KEY is missing!")

        self.base_url = "https://api.tiingo.com"

        # settings.py의 외장하드 경로 사용
        self.forex_path = DIRS['forex']

        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime.now()

        self.forex_pairs = {
            'EURUSD': 'Euro/USD', 'GBPUSD': 'British Pound/USD', 'USDJPY': 'USD/Japanese Yen',
            'USDCHF': 'USD/Swiss Franc', 'AUDUSD': 'Australian Dollar/USD', 'USDCAD': 'USD/Canadian Dollar'
        }
        logging.info(f"Forex Collector initialized. Target Dir: {self.forex_path}")

    def get_forex_data(self, pair, start_date, end_date):
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        url = f"{self.base_url}/tiingo/fx/{pair}/prices"
        params = {'token': self.api_key, 'startDate': start_str, 'endDate': end_str, 'resampleFreq': '1day'}

        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)
                    df['date'] = pd.to_datetime(df['date'])
                    df.set_index('date', inplace=True)
                    logging.info(f"✅ {pair}: Fetched {len(df)} rows.")
                    return df[['close']].rename(columns={'close': 'Close'})
            else:
                logging.error(f"{pair}: HTTP {response.status_code}")
        except Exception as e:
            logging.error(f"{pair}: Error: {e}")
        return None

    def run_collection(self):
        logging.info("=== Starting Forex Collection ===")
        for pair, name in self.forex_pairs.items():
            filename = self.forex_path / f"{pair}.csv"
            start_date = self.start_date

            existing_df = None
            if filename.exists():
                try:
                    existing_df = pd.read_csv(filename, index_col=0, parse_dates=True)
                    last_date = existing_df.index.max()
                    if last_date >= datetime.now() - timedelta(days=1):
                        logging.info(f"⏭️ {pair}: Already up-to-date.")
                        continue
                    start_date = last_date + timedelta(days=1)
                except Exception as e:
                    logging.warning(f"Read error {pair}: {e}. Fetching all.")

            new_data = self.get_forex_data(pair, start_date, self.end_date)

            if new_data is not None and not new_data.empty:
                if existing_df is not None:
                    combined_df = pd.concat([existing_df, new_data])
                    combined_df = combined_df[~combined_df.index.duplicated(keep='first')].sort_index()
                else:
                    combined_df = new_data

                combined_df.to_csv(filename)
                logging.info(f"💾 {pair}: Saved to {filename}")

            time.sleep(1.5)  # API 딜레이
        logging.info("=== Finished ===")


if __name__ == "__main__":
    ForexSimpleCollector().run_collection()