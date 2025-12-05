import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드 (API 키 등)
load_dotenv()

# 1. 프로젝트 루트 경로 (자동 인식)
# config/settings.py 위치에서 두 단계 위가 프로젝트 루트
BASE_DIR = Path(__file__).resolve().parent.parent

# 2. 데이터 저장 루트 (외장하드 설정)
# 외장하드가 연결되어 있으면 거기로, 아니면 프로젝트 내부 data 폴더로 fallback
EXTERNAL_DB_PATH = Path("/Volumes/Postgres_DB")
if EXTERNAL_DB_PATH.exists():
    DATA_ROOT = EXTERNAL_DB_PATH / "economic_data"
else:
    DATA_ROOT = BASE_DIR / "data"

# 3. 하위 디렉토리 설정
RAW_DIR = DATA_ROOT / "01_raw"
PROCESSED_DIR = DATA_ROOT / "02_processed"
LOG_DIR = BASE_DIR / "logs"  # 로그는 로컬에 남기는 게 확인하기 편함

# 카테고리별 상세 경로
DIRS = {
    'etf': RAW_DIR / "etfs",
    'forex': RAW_DIR / "forex",
    'fred': RAW_DIR / "fred_indicators",
    'events': RAW_DIR / "events",
}

# 4. API 키 중앙 관리
API_KEYS = {
    'TIINGO': os.getenv('TIINGO_API_KEY'),
    'FRED': os.getenv('FRED_API-KEY'),
    'FINNHUB': os.getenv('FINNHUB_API_KEY')
}

# 5. DB 연결 정보 (나중을 위해 미리 준비)
DB_CONFIG = {
    'dbname': 'economy_db',
    'user': 'xodh3',
    'host': 'localhost',
    'port': 5432
}

# 6. 초기화 함수 (폴더 생성)
def init_directories():
    """필요한 모든 폴더를 생성합니다."""
    for path in [RAW_DIR, PROCESSED_DIR, LOG_DIR] + list(DIRS.values()):
        path.mkdir(parents=True, exist_ok=True)
    print(f"✅ Data Directory Initialized at: {DATA_ROOT}")

# 실행 시 폴더 체크
init_directories()