import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 상위 폴더 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# .env 파일 로딩
load_dotenv()

# 클라우드 주소 가져오기
DB_URI = os.getenv("SUPABASE_DB_URI")


def test_connection():
    if not DB_URI:
        print("❌ .env 파일에서 SUPABASE_DB_URI를 찾을 수 없습니다!")
        return

    print(f"🌐 클라우드 DB 접속 시도 중... (주소: {DB_URI[:20]}...)")

    try:
        # 연결 시도
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            # 간단한 쿼리 실행 (DB 버전 확인)
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print("✅ 연결 성공! 🎉")
            print(f"🐘 DB 정보: {version}")

    except Exception as e:
        print("❌ 연결 실패... 주소와 비밀번호를 다시 확인해주세요.")
        print(f"에러 내용: {e}")


if __name__ == "__main__":
    test_connection()