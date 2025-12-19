import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

# 1. 두 개의 DB 주소 준비
CLOUD_DB_URI = os.getenv("SUPABASE_DB_URI")
# 로컬 DB 주소 (TablePlus 접속 정보와 동일)
LOCAL_DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

if not CLOUD_DB_URI:
    print("❌ .env에서 SUPABASE_DB_URI를 찾을 수 없습니다.")
    sys.exit(1)


def sync_table(table_name):
    print(f"\n🔄 [{table_name}] 동기화 시작 (Cloud -> Local)...")

    cloud_engine = create_engine(CLOUD_DB_URI)
    local_engine = create_engine(LOCAL_DB_URI)

    try:
        # 데이터가 있는지 먼저 확인
        with cloud_engine.connect() as conn:
            # 테이블 존재 여부 확인 쿼리 (PostgreSQL)
            exists = conn.execute(text(f"SELECT to_regclass('public.{table_name}')")).scalar()
            if not exists:
                print(f"   ⚠️ 클라우드에 '{table_name}' 테이블이 없습니다. 건너뜁니다.")
                return

        # 청크 단위로 읽어서 메모리 터짐 방지
        df_iterator = pd.read_sql(f"SELECT * FROM {table_name}", cloud_engine, chunksize=50000)

        first_chunk = True
        total_rows = 0

        for df_chunk in df_iterator:
            mode = 'replace' if first_chunk else 'append'
            df_chunk.to_sql(table_name, local_engine, if_exists=mode, index=False)
            total_rows += len(df_chunk)
            print(f"   📥 {total_rows:,}개 행 복사 중...")
            first_chunk = False

        if total_rows == 0:
            print(f"   ⚠️ 데이터가 비어있습니다.")
        else:
            print(f"   💾 로컬 DB 저장 완료! (총 {total_rows:,}개)")

        # 중복 방지 규칙(Unique Key) 다시 걸어주기
        # (테이블마다 유니크 키 조건이 다르므로, 여기서는 대표적인 것만 처리하거나 생략)
        if table_name == 'market_price_daily':
            with local_engine.connect() as conn:
                try:
                    conn.execute(text(
                        f"ALTER TABLE {table_name} ADD CONSTRAINT unique_{table_name} UNIQUE (symbol, trade_date)"))
                    conn.commit()
                    print("   🔒 중복 방지 규칙 설정 완료.")
                except:
                    pass  # 이미 있으면 패스

    except Exception as e:
        print(f"   ❌ '{table_name}' 동기화 실패: {e}")


if __name__ == "__main__":
    # 👇👇👇 여기에 복사하고 싶은 테이블 이름을 다 적으세요! 👇👇👇
    tables_to_sync = [
        "indicator_metadata",  # (필수) 지표 설명서
        "macro_time_series",  # (필수) 경제 지표 데이터
        "market_price_daily",  # (필수) 주가 데이터
        "practice_spy",  # (선택) 예전 연습용 (필요 없으면 지워도 됨)
        # "temp_tiingo_data"   # (비추천) 임시 쓰레기통이라 복사 안 함
    ]

    for table in tables_to_sync:
        sync_table(table)

    print("\n🎉 지정한 모든 테이블의 동기화가 완료되었습니다!")