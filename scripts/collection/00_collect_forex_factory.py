import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time
import sys
import logging
from pathlib import Path

# Selenium import
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- [설정 파일 연동] ---
# 현재 파일 위치: scripts/collection/00_collect_forex_factory.py
# 루트 경로(economic_analysis)를 찾아서 sys.path에 추가
FILE = Path(__file__).resolve()
ROOT = FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from config.settings import DIRS, LOG_DIR

# --- [로깅 설정] ---
log_file = LOG_DIR / 'collect_forex_factory.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)


def run_scraper():
    today = date.today()
    # 이번 달과 지난달 URL 생성
    this_month_url = f"https://www.forexfactory.com/calendar?month={today.strftime('%b').lower()}.{today.year}"
    last_month_date = today - relativedelta(months=1)
    last_month_url = f"https://www.forexfactory.com/calendar?month={last_month_date.strftime('%b').lower()}.{last_month_date.year}"

    urls_to_scrape = [this_month_url, last_month_url]

    # --- Browser Setup (Headless Mode) ---
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # 화면 없이 실행
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

    logging.info("Starting Selenium scraper...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    all_calendar_data = []

    for url in urls_to_scrape:
        logging.info(f"Processing URL: {url}")
        try:
            driver.get(url)
            # 테이블이 로딩될 때까지 최대 15초 대기
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "calendar__table"))
            )
            time.sleep(2)

            rows = driver.find_elements(By.CSS_SELECTOR, 'tr.calendar__row')
            current_date = None

            for row in rows:
                # 날짜 파싱 (날짜는 섹션의 첫 줄에만 있음)
                date_elements = row.find_elements(By.CSS_SELECTOR, 'td.calendar__date')
                if date_elements:
                    date_text = date_elements[0].text.strip()
                    if date_text:
                        try:
                            current_date = datetime.strptime(f"{date_text} {url.split('.')[-1]}", '%a %b %d %Y').date()
                        except (ValueError, IndexError):
                            pass

                # 시간이 없는 행(공휴일 등) 제외
                time_elements = row.find_elements(By.CSS_SELECTOR, 'td.calendar__time')
                if not time_elements or not time_elements[0].text.strip():
                    continue

                time_text = time_elements[0].text.strip()
                if not current_date: continue

                try:
                    country = row.find_element(By.CSS_SELECTOR, 'td.calendar__currency').text.strip()
                    event = row.find_element(By.CSS_SELECTOR, 'td.calendar__event').text.strip()
                    actual = row.find_element(By.CSS_SELECTOR, 'td.calendar__actual').text.strip()
                    forecast = row.find_element(By.CSS_SELECTOR, 'td.calendar__forecast').text.strip()
                    previous = row.find_element(By.CSS_SELECTOR, 'td.calendar__previous').text.strip()
                    impact_element = row.find_element(By.CSS_SELECTOR, 'td.calendar__impact span')
                    importance = impact_element.get_attribute('title')

                    full_datetime = f"{current_date} {time_text}"

                    all_calendar_data.append({
                        'datetime': full_datetime,
                        'country': country,
                        'event': event,
                        'importance': importance,
                        'actual': actual,
                        'forecast': forecast,
                        'previous': previous,
                    })
                except Exception:
                    continue

        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")

    driver.quit()
    logging.info("Scraping finished. Saving data...")

    if not all_calendar_data:
        logging.warning("No data scraped.")
        return

    df = pd.DataFrame(all_calendar_data)

    # USD 관련 이벤트만 필터링
    df_usa = df[df['country'] == 'USD'].copy()

    if not df_usa.empty:
        df_usa['datetime'] = pd.to_datetime(df_usa['datetime'], format='%Y-%m-%d %I:%M%p', errors='coerce')

        # [중요] 외장하드 events 폴더에 저장
        output_filename = DIRS['events'] / 'forex_factory_usd_recent.csv'

        try:
            df_usa.to_csv(output_filename, index=False, encoding='utf-8-sig')
            logging.info(f"✅ Saved to: {output_filename}")
        except Exception as e:
            logging.error(f"❌ Save failed: {e}")
    else:
        logging.info("No USD events found.")


if __name__ == "__main__":
    run_scraper()