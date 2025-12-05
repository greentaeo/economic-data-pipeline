import sys
import logging
import pandas as pd
from fredapi import Fred
from pathlib import Path

# --- [설정 파일 연동] ---
FILE = Path(__file__).resolve()
ROOT = FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config.settings import API_KEYS, DIRS, LOG_DIR

# 같은 폴더에 있는 indicators.py 임포트
try:
    from scripts.collection.indicators import fred_indicators
except ImportError:
    # 경로 문제시 fallback
    sys.path.append(str(FILE.parent))
    from indicators import fred_indicators

# --- [로깅 설정] ---
log_file = LOG_DIR / 'collect_fred_data.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


def fetch_fred_data(fred_conn, indicators):
    """FRED 데이터를 수집하여 외장하드에 저장"""
    # 외장하드 경로: /Volumes/Postgres_DB/economic_data/01_raw/fred_indicators
    base_path = DIRS['fred']

    for category, series_list in indicators.items():
        logging.info(f"--- Processing Category: {category} ---")

        # 카테고리별 폴더 자동 생성 (예: macro, employment...)
        category_path = base_path / category
        category_path.mkdir(parents=True, exist_ok=True)

        for indicator_info in series_list:
            try:
                series_id = indicator_info['id']
                # 데이터 수집
                logging.info(f"Fetching: {series_id}")
                data = fred_conn.get_series(series_id)

                file_name = f"{series_id}.csv"
                output_path = category_path / file_name

                # 저장
                data.to_frame(name=series_id).to_csv(output_path)
                logging.info(f"✅ Saved: {output_path}")

            except Exception as e:
                failed_id = indicator_info.get('id', 'Unknown')
                logging.error(f"❌ Failed {failed_id}: {e}")


def main():
    logging.info("🚀 FRED Data Collection Start")

    # settings.py에서 API 키 가져오기
    fred_key = API_KEYS['FRED']
    if not fred_key:
        logging.error("FRED_API_KEY missing in .env/settings.")
        return

    try:
        fred = Fred(api_key=fred_key)
        fetch_fred_data(fred, fred_indicators)
    except Exception as e:
        logging.error(f"FRED Critical Error: {e}")

    logging.info("🎉 FRED Collection Finished")


if __name__ == '__main__':
    main()
