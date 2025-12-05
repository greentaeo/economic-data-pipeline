import pandas as pd
from sqlalchemy import create_engine, text

# 1. DB 접속 정보 설정 (전화번호부)
# 형식: postgresql+psycopg2://사용자ID:비밀번호@주소:포트/DB이름
# (비밀번호가 없으면 생략 가능하지만, 보통은 설정합니다. 일단 없이 시도해봅시다.)
DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

def test_connection():
    print("Connecting to DB...")
    try :
        # 2. 엔진 시동 걸기(연결준비)
        engine = create_engine(DB_URI)

        # 2. 진짜로 연결해서 연결시도
        with engine.connect() as conn:
            # 간단한 인사말(버전확인) 쿼리날리기
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print("✅ 접속 성공!")
            print(f"Database version is {version}")
            # 4. 우리가 만든 테이블도 잘있나 확인
            print("\n📋 테이블 목록 확인:")
            query = text("SELECT * FROM practice_spy order by close_price desc limit 5")
            tables = conn.execute(query).fetchall()
            for table in tables:
                print(f"- {table[0]} {table[1]} {table[2]}")

    except Exception as e :
        print("❌ 접속 실패! 에러 메시지를 확인하세요:")
        print(e)

if __name__ == "__main__":
    test_connection()
