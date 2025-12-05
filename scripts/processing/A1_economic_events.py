import pandas as pd
from pathlib import Path
import logging
# yfinance_tickers를 import에서 제거하고 fred_indicators만 가져옵니다.
from scripts.collection.indicators import fred_indicators

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_economic_events():
    """
    FRED CSV 파일에서 경제 이벤트 날짜를 추출하는 함수 (yfinance 제외)
    """
    # --- 한글 이름 매핑 딕셔너리 생성 (FRED만 사용) ---
    id_to_name_map = {}
    for category, indicators_list in fred_indicators.items():
        for indicator in indicators_list:
            id_to_name_map[indicator['id']] = indicator['name_kr']

    # --- 경로 설정 ---
    project_root = Path.cwd()
    if project_root.name == 'scripts':
        project_root = project_root.parent
    data_path = project_root / 'data' / 'raw'
    events_path = project_root / 'data' / 'events'
    events_path.mkdir(exist_ok=True, parents=True)

    # --- 이벤트 데이터 추출 ---
    all_events = []  # This list will store all the event dictionaries

    # data/raw 폴더 안의 모든 카테고리 폴더를 순회합니다.
    for category_folder in data_path.iterdir():
        if category_folder.is_dir() and category_folder.name in fred_indicators:
            category = category_folder.name
            logging.info(f"Processing category: {category}")

            for csv_file in category_folder.glob('*.csv'):
                try:
                    indicator_id = csv_file.stem
                    df = pd.read_csv(csv_file, index_col=0, parse_dates=True)

                    for date in df.index:
                        all_events.append({
                            'date': date,
                            'indicator_id': indicator_id,
                            'name_kr': id_to_name_map.get(indicator_id, indicator_id),  # 한글 이름 추가
                            'category': category
                        })
                except Exception as e:
                    logging.error(f"Error processing {csv_file.name}: {e}")

    # --- 데이터프레임 생성 및 저장 ---
    if not all_events:
        logging.warning("No events were extracted. Please check if raw data files exist.")
        return pd.DataFrame()

    events_df = pd.DataFrame(all_events)
    events_df['date'] = pd.to_datetime(events_df['date'])
    events_df = events_df.sort_values('date').reset_index(drop=True)

    events_file = events_path / 'economic_events.csv'
    events_df.to_csv(events_file, index=False)
    logging.info(f"Successfully saved {len(events_df)} events to {events_file}")

    return events_df


if __name__ == "__main__":
    extract_economic_events()