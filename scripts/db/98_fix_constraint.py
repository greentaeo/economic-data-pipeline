import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 경로 설정
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
if not DB_URI:
    print("❌ DB 연결 정보를 찾을 수 없습니다.")
    sys.exit(1)


def add_unique_constraint():
    print("🔧 DB 중복 방지 규칙(Unique Constraint) 추가 중...")
    engine = create_engine(DB_URI)

    with engine.connect() as conn:
        try:
            # 1. 기존 중복 데이터 제거 (ctid 사용)
            # ctid는 PostgreSQL이 내부적으로 행을 구분하는 주소입니다.
            print("   🧹 기존 중복 데이터 정리 중 (ctid 사용)...")
            deduplicate_query = text("""
            DELETE FROM market_price_daily a USING market_price_daily b
            WHERE a.ctid < b.ctid AND a.symbol = b.symbol AND a.trade_date = b.trade_date;
            """)
            conn.execute(deduplicate_query)
            conn.commit()

            # 2. 유니크 제약조건 추가
            print("   🔒 유니크 제약조건(Symbol + Trade_Date) 설정 중...")
            constraint_query = text("""
            ALTER TABLE market_price_daily
            ADD CONSTRAINT unique_symbol_date UNIQUE (symbol, trade_date);
            """)
            conn.execute(constraint_query)
            conn.commit()
            print("   ✅ 성공! 이제 'ON CONFLICT' 기능이 정상 작동합니다.")

        except Exception as e:
            if "already exists" in str(e):
                print("   ⚠️ 이미 규칙이 설정되어 있습니다. (문제 없음)")
            else:
                print(f"   ❌ 오류 발생: {e}")


if __name__ == "__main__":
    add_unique_constraint()