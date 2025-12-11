import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
SOURCE_FILE = "data/01_raw/metadata/country_United_States.csv"  # 파일명 확인 필요
TABLE_NAME = "indicator_metadata"


def load_metadata():
    print(f"🚀 메타데이터(설명서) 적재 시작!")
    engine = create_engine(DB_URI)

    if not os.path.exists(SOURCE_FILE):
        # 파일명이 다를 수 있으니 metadata 폴더의 첫 번째 csv를 찾음
        import glob
        files = glob.glob("data/01_raw/metadata/*.csv")
        if not files:
            print("❌ 메타데이터 파일이 없습니다.")
            return
        file_path = files[0]
    else:
        file_path = SOURCE_FILE

    try:
        df = pd.read_csv(file_path)

        # 컬럼 매핑 (CSV -> DB 테이블)
        # CSV: HistoricalDataSymbol -> DB: indicator_symbol
        rename_map = {
            'HistoricalDataSymbol': 'indicator_symbol',
            'Title': 'title',
            'Country': 'country',
            'Unit': 'unit',
            'Source': 'source',
            'Category': 'category',
            'Frequency': 'frequency'
        }
        df = df.rename(columns=rename_map)

        # 필요한 컬럼만 선택
        available_cols = [c for c in rename_map.values() if c in df.columns]
        final_df = df[available_cols].dropna(subset=['indicator_symbol'])

        final_df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print(f"🎉 메타데이터 {len(final_df)}건 저장 완료!")

    except Exception as e:
        print(f"❌ 에러 발생: {e}")


if __name__ == "__main__":
    load_metadata()