import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
load_dotenv()

DB_URI = os.getenv("SUPABASE_DB_URI")
if not DB_URI:
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"


def check_status():
    engine = create_engine(DB_URI)
    print("📊 [Supabase DB 현황 보고서]")
    print("-" * 40)

    with engine.connect() as conn:
        # 1. 가격 데이터 확인
        try:
            res = conn.execute(text("SELECT count(*), min(trade_date), max(trade_date) FROM market_price_daily"))
            row = res.fetchone()
            print(f"💰 가격 데이터(Market Price): {row[0]:,}개")
            print(f"   📅 기간: {row[1]} ~ {row[2]}")
        except:
            print("💰 가격 데이터: 테이블 없음")

        print("-" * 20)

        # 2. 경제 지표 확인
        try:
            res = conn.execute(text("SELECT count(*), count(distinct indicator_symbol) FROM macro_time_series"))
            row = res.fetchone()
            print(f"📈 경제 지표(Macro Series): {row[0]:,}개")
            print(f"   🗂 지표 종류: {row[1]}개")
        except:
            print("📈 경제 지표: 테이블 없음")

        print("-" * 20)

        # 3. 메타데이터 확인
        try:
            res = conn.execute(text("SELECT count(*) FROM indicator_metadata"))
            count = res.fetchone()[0]
            print(f"📝 지표 설명서(Metadata): {count}개")
        except:
            print("📝 지표 설명서: 테이블 없음")

    print("-" * 40)
    print("🎉 데이터 이사 작업 완료!")


if __name__ == "__main__":
    check_status()