import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import glob

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
if not DB_URI:
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

SOURCE_DIR = "data/01_raw/macro_series"
TABLE_NAME = "macro_time_series"


def try_read_csv(file_path):
    encodings = ['utf-8', 'cp949', 'euc-kr', 'latin1']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except UnicodeDecodeError:
            continue
    return None


def load_macro_data():
    print(f"🚀 [v3] 경제 지표 적재 시작! (스마트 컬럼 감지)")
    engine = create_engine(DB_URI)
    files = glob.glob(os.path.join(SOURCE_DIR, "*.csv"))

    success_count = 0

    for i, file_path in enumerate(files):
        file_name = os.path.basename(file_path)
        # 심볼 정리
        symbol = file_name.replace(".csv", "").upper()
        if "HISTORICAL_COUNTRY_" in symbol:
            symbol = symbol.replace("HISTORICAL_COUNTRY_", "").replace("_INDICATOR_", "_")
            # 끝에 붙은 _ 제거
            if symbol.endswith("_"): symbol = symbol[:-1]

        try:
            df = try_read_csv(file_path)
            if df is None:
                continue

            # 컬럼명 정리 (소문자, 공백제거)
            df.columns = [str(c).strip().lower() for c in df.columns]

            # --- [핵심 수정] 컬럼 찾기 로직 강화 ---

            # 1. 날짜 컬럼 찾기
            date_col = None
            date_candidates = ['date', 'datetime', 'time', 'observation_date', 'period']

            # (1) 이름으로 찾기
            for cand in date_candidates:
                if cand in df.columns:
                    date_col = cand
                    break
            # (2) 없으면 첫 번째 컬럼이 날짜일 확률 높음
            if not date_col and len(df.columns) > 0:
                date_col = df.columns[0]

            # 2. 값(Value) 컬럼 찾기
            val_col = None
            val_candidates = ['value', 'actual', 'close', 'price', 'last', symbol.lower()]

            # (1) 이름으로 우선 찾기 (Value, Actual 등)
            for cand in val_candidates:
                if cand in df.columns:
                    val_col = cand
                    break

            # (2) 이름으로 못 찾았으면, '숫자형' 데이터가 있는 컬럼 찾기
            if not val_col:
                for col in df.columns:
                    if col == date_col: continue
                    # 문자열이면 건너뛰고, 숫자면 선택
                    if pd.api.types.is_numeric_dtype(df[col]):
                        val_col = col
                        break

            # (3) 그래도 없으면? (데이터가 문자열로 되어있을 수도 있음) -> 날짜 아닌 것 중 'Country' 같은 거 제외하고 선택
            if not val_col:
                exclude_keywords = ['country', 'category', 'freq', 'symbol', 'unit', 'source']
                for col in df.columns:
                    if col == date_col: continue
                    if any(x in col for x in exclude_keywords): continue
                    val_col = col  # 이거다 싶으면 선택
                    break

            if not date_col or not val_col:
                print(f"   ⚠️ {symbol}: 컬럼 인식 실패 (Date: {date_col}, Val: {val_col}) -> 건너뜀")
                continue

            # 데이터 변환
            df['date_time'] = pd.to_datetime(df[date_col], errors='coerce')

            # 값 변환 (콤마 제거 후 숫자 변환)
            if df[val_col].dtype == object:
                df['value'] = pd.to_numeric(df[val_col].astype(str).str.replace(',', ''), errors='coerce')
            else:
                df['value'] = pd.to_numeric(df[val_col], errors='coerce')

            df['indicator_symbol'] = symbol

            # 국가 정보 추론
            if 'country' in df.columns:
                # 첫 번째 행의 국가 정보를 가져옴 (보통 파일 전체가 한 국가)
                country_val = df['country'].iloc[0] if not df.empty else 'Unknown'
                df['country'] = country_val
            elif "KOREA" in symbol:
                df['country'] = "South Korea"
            else:
                df['country'] = "United States"

            # 필요한 데이터만 남기기
            final_df = df[['date_time', 'indicator_symbol', 'value', 'country']].dropna(subset=['date_time', 'value'])

            if not final_df.empty:
                final_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, chunksize=1000)
                # print(f"   ✅ {symbol}: {len(final_df)}개 저장 완료")
                success_count += 1
            else:
                print(f"   ⚠️ {symbol}: 변환 후 데이터 없음 (모두 NaN?)")

        except Exception as e:
            print(f"   ❌ {symbol} 에러: {e}")

    print(f"\n🎉 총 {len(files)}개 중 {success_count}개 파일 적재 완료!")


if __name__ == "__main__":
    load_macro_data()