import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# 1. 두 개의 금고 주소 준비
load_dotenv()
LOCAL_DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"
CLOUD_DB_URI = os.getenv("SUPABASE_DB_URI")  # .env에서 가져옴


def migrate_data():
    print("🚀 데이터 이사를 시작합니다! (Local -> Cloud)")

    # 연결 엔진 생성
    local_engine = create_engine(LOCAL_DB_URI)
    cloud_engine = create_engine(CLOUD_DB_URI)

    # 2. 로컬에서 데이터 꺼내기 (SELECT)
    print("📦 [1/3] 로컬 DB에서 데이터를 포장하는 중...")
    try:
        # 캔들차트용 데이터(practice_spy) 가져오기
        df = pd.read_sql("SELECT * FROM practice_spy ORDER BY trade_date ASC", local_engine)
        print(f"   👉 총 {len(df)}개의 데이터를 찾았습니다.")
    except Exception as e:
        print(f"❌ 로컬 DB 읽기 실패: {e}")
        return

    # 3. 클라우드에 테이블 만들기 (없으면 생성)
    print("🏗️ [2/3] 클라우드 DB에 테이블을 건설하는 중...")
    # 'practice_spy' 테이블의 구조를 그대로 복사해서 만듭니다.
    # index=False: 판다스 인덱스 숫자는 저장 안 함
    # if_exists='replace': 이미 있으면 덮어쓰기 (처음이니까 확실하게!)
    try:
        df.to_sql('practice_spy', cloud_engine, if_exists='replace', index=False, chunksize=500)
        print("   👉 테이블 생성 및 데이터 전송 완료!")
    except Exception as e:
        print(f"❌ 클라우드 전송 실패: {e}")
        return

    # 4. 마무리 확인
    print("✅ [3/3] 이사 완료! 클라우드 DB 확인 중...")
    with cloud_engine.connect() as conn:
        result = conn.execute(text("SELECT count(*) FROM practice_spy"))
        count = result.fetchone()[0]
        print(f"🎉 클라우드 DB에 현재 {count}개의 데이터가 저장되었습니다.")


if __name__ == "__main__":
    migrate_data()