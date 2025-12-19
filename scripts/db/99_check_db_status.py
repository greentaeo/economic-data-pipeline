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

        # 1. 가격 데이터 전체 확인
        try:
            res = conn.execute(text("SELECT count(*), min(trade_date), max(trade_date) FROM market_price_daily"))
            row = res.fetchone()
            print(f"💰 가격 데이터(Market Price): {row[0]:,}개")
            print(f"   📅 기간: {row[1]} ~ {row[2]}")
        except:
            print("💰 가격 데이터: 테이블 없음")

        print("-" * 20)

        # 2. QQQ OHLCV 누락 데이터 확인 (핵심)
        try:
            # QQQ 종목 중, OHLCV 컬럼 중 하나라도 NULL인 행의 개수를 셉니다.
            query = text("""
                SELECT 
                    COUNT(*) 
                FROM market_price_daily 
                WHERE symbol = 'QQQ' AND (
                    open_price IS NULL OR 
                    high_price IS NULL OR 
                    low_price IS NULL OR 
                    volume IS NULL
                )
            """)
            null_count = conn.execute(query).scalar()

            # QQQ 총 행 개수를 셉니다.
            total_count = conn.execute(text("SELECT COUNT(*) FROM market_price_daily WHERE symbol = 'QQQ'")).scalar()

            print(f"🔍 QQQ 데이터 상태 보고:")
            print(f"   - 총 행 개수: {total_count:,}개")

            if null_count > 0:
                print(f"   🚨 **누락된 OHLCV 행:** {null_count:,}개 (캔들스틱 차트 오류 원인!)")
                print("   **조치:** 03_tiingo_etf_collector.py 실행 필요")
            else:
                print("   ✅ OHLCV 누락 없음. QQQ 데이터 상태 양호.")

        except Exception as e:
            print(f"🔍 QQQ 데이터 확인 실패: {e}")

        print("-" * 20)

        # 3. 경제 지표 확인 (나머지 부분은 그대로)
        try:
            res = conn.execute(text("SELECT count(*), count(distinct indicator_symbol) FROM macro_time_series"))
            row = res.fetchone()
            print(f"📈 경제 지표(Macro Series): {row[0]:,}개")
            print(f"   🗂 지표 종류: {row[1]}개")
        except:
            print("📈 경제 지표: 테이블 없음")

        print("-" * 40)
        print("🎉 DB 점검 완료!")


if __name__ == "__main__":
    check_status()