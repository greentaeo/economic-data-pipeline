import sys
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# --- [경로 설정] ---
# scripts/utils.py를 불러오기 위해 상위 폴더 경로 추가
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from scripts.utils import send_discord_alert  # 방금 만든 알림 함수 가져오기
from config.settings import DB_CONFIG  # DB 설정 가져오기 (있다면)

# DB 접속 정보 (직접 입력 혹은 settings 사용)
DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"


def check_market_signal():
    engine = create_engine(DB_URI)

    # 분석 대상 (나스닥 QQQ)
    ticker = 'QQQ'

    print(f"🔍 {ticker} 신호 분석 중...")

    # 1. DB에서 데이터 가져오기 (최근 60일치면 충분)
    query = f"""
    SELECT trade_date, close_price 
    FROM practice_spy 
    WHERE ticker = '{ticker}' 
    ORDER BY trade_date ASC
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("❌ 데이터가 없습니다.")
        return

    # 2. 이동평균선 계산 (우리가 찾은 최적값: 4일 vs 37일)
    df['MA_Short'] = df['close_price'].rolling(window=4).mean()
    df['MA_Long'] = df['close_price'].rolling(window=37).mean()

    # 3. 오늘의 상태 확인 (마지막 데이터)
    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    date_str = today['trade_date'].strftime('%Y-%m-%d')
    current_price = today['close_price']

    # 메시지 기본 틀
    message = f"📊 **[{date_str}] {ticker} 시장 분석**\n현재가: ${current_price:.2f}\n"

    # 4. 골든크로스 판독 (매수 신호)
    # 어제는 단기가 장기보다 낮았는데(<=), 오늘은 뚫고 올라감(>)
    if (yesterday['MA_Short'] <= yesterday['MA_Long']) and (today['MA_Short'] > today['MA_Long']):
        message += "\n🔥 **[골든크로스 발생!] 강력 매수 신호** 🔥\n"
        message += f"단기선(4일)이 장기선(37일)을 돌파했습니다.\n추세가 상승으로 전환되었습니다!"
        # 중요하니까 알림 발송!
        send_discord_alert(message)
        print("✅ 매수 신호 발송 완료")

    # 5. 데드크로스 판독 (매도 신호)
    # 어제는 높았는데(>=), 오늘은 뚫고 내려감(<)
    elif (yesterday['MA_Short'] >= yesterday['MA_Long']) and (today['MA_Short'] < today['MA_Long']):
        message += "\n❄️ **[데드크로스 발생] 매도/현금화 신호** ❄️\n"
        message += f"단기선이 무너졌습니다. 리스크 관리가 필요합니다."
        # 중요하니까 알림 발송!
        send_discord_alert(message)
        print("✅ 매도 신호 발송 완료")

    else:
        # 6. 특이사항 없음 (그냥 추세 유지 중)
        # 평소에는 너무 시끄러우니까 메시지를 안 보내거나, '로그'만 남깁니다.
        status = "상승 추세 유지 중 📈" if today['MA_Short'] > today['MA_Long'] else "하락 추세(관망) 📉"
        print(f"ℹ️ 특이 신호 없음. 현재 상태: {status}")

        # (테스트용) 오늘은 신호가 없어도 확인차 한번 보내봅시다.
        # 나중엔 주석 처리하세요.
        send_discord_alert(f"{message}\n특이 신호 없음. ({status})")


if __name__ == "__main__":
    check_market_signal()