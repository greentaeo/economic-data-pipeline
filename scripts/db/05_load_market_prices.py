import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
if not DB_URI:
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

SOURCE_DIR = "data/01_raw/market_price"
TABLE_NAME = "market_price_daily"


def try_read_csv(file_path):
    """여러 인코딩으로 파일 읽기 시도 (utf-16 추가!)"""
    # 엑셀 CSV는 utf-16인 경우가 많음
    encodings = ['utf-8', 'utf-16', 'cp949', 'euc-kr', 'latin1']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except UnicodeError:
            continue
    return None


def clean_column_names(df):
    # 1. 컬럼명 소문자 및 공백/특수문자 제거
    df.columns = [str(c).strip().lower().replace(" ", "_").replace(".", "") for c in df.columns]

    # 2. 강력한 매핑
    rename_map = {
        'date': 'trade_date', '날짜': 'trade_date', 'datetime': 'trade_date', 'observation_date': 'trade_date',
        'price': 'close_price', 'close': 'close_price', '종가': 'close_price', 'last': 'close_price',
        'value': 'close_price',
        'open': 'open_price', '시가': 'open_price',
        'high': 'high_price', '고가': 'high_price',
        'low': 'low_price', '저가': 'low_price',
        'vol': 'volume', 'volume': 'volume', '거래량': 'volume'
    }
    df = df.rename(columns=rename_map)
    return df


def process_and_load():
    print(f"🚀 [v2.2] 가격 데이터 적재 (인코딩/단일컬럼 해결) (대상: {SOURCE_DIR})")
    engine = create_engine(DB_URI)
    files = glob.glob(os.path.join(SOURCE_DIR, "*.csv"))

    success_count = 0

    for i, file_path in enumerate(files):
        file_name = os.path.basename(file_path)
        symbol = file_name.replace(".csv", "").upper()
        if "MARKETS_HISTORICAL_" in symbol:
            symbol = symbol.replace("MARKETS_HISTORICAL_", "").replace("_CUR", "").replace("_IND", "").replace("_COM",
                                                                                                               "")

        try:
            # 1. 파일 읽기
            df = try_read_csv(file_path)
            if df is None:
                print(f"   ❌ {symbol}: 파일 읽기 실패 (알 수 없는 인코딩)")
                continue

            # 2. 컬럼 정리
            df = clean_column_names(df)

            # 3. 필수 컬럼 확인 (trade_date)
            if 'trade_date' not in df.columns:
                # 첫 번째 컬럼을 날짜로 가정
                df.rename(columns={df.columns[0]: 'trade_date'}, inplace=True)

            # 4. [핵심] 가격(Close) 컬럼 찾기 전략
            # (1) 이미 매핑된 'close_price'가 있는지 확인
            if 'close_price' not in df.columns:
                # (2) Open/High/Low 중에라도 있는지 확인
                found = False
                for alt in ['open_price', 'high_price', 'low_price']:
                    if alt in df.columns:
                        df['close_price'] = df[alt]
                        found = True
                        break

                # (3) [NEW] 그래도 없으면? (DGS10 처럼 이름이 자기 자신인 경우)
                # 날짜가 아니고, 숫자인 컬럼을 찾아서 'close_price'로 쓴다.
                if not found:
                    for col in df.columns:
                        if col == 'trade_date': continue
                        # 해당 컬럼 이름에 symbol이 포함되어 있거나, 그냥 남는 컬럼이면 채택
                        # 여기서는 단순하게 "날짜 빼고 첫 번째 컬럼"을 가격으로 간주
                        df['close_price'] = df[col]
                        found = True
                        break

            if 'close_price' not in df.columns:
                print(f"   ⚠️ {symbol}: 가격 컬럼을 도저히 못 찾음. (컬럼: {list(df.columns)})")
                continue

            # 5. 데이터 타입 변환
            df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
            df = df.dropna(subset=['trade_date'])

            # 숫자 변환
            cols_to_numeric = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for col in cols_to_numeric:
                if col in df.columns:  # 컬럼이 있을 때만
                    if df[col].dtype == object:
                        df[col] = df[col].astype(str).str.replace(',', '').apply(pd.to_numeric, errors='coerce')

            df['symbol'] = symbol

            # DB 컬럼 맞추기
            db_cols = ['trade_date', 'symbol', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']
            for col in db_cols:
                if col not in df.columns:
                    df[col] = None

            final_df = df[db_cols]

            if not final_df.empty:
                final_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=1000)
                # print(f"   ✅ {symbol}: {len(final_df)}개 저장 완료.")
                success_count += 1
            else:
                print(f"   ⚠️ {symbol}: 유효한 데이터 없음")

        except Exception as e:
            print(f"   ❌ {symbol} 에러: {e}")

    print(f"\n🎉 총 {len(files)}개 중 {success_count}개 파일 적재 완료!")


if __name__ == "__main__":
    process_and_load()