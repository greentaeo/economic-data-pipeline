import os
import requests
import logging
from dotenv import load_dotenv

# 1. 숨겨둔 비밀번호(.env) 꺼내오기
load_dotenv()
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")


def send_discord_alert(message: str):
    """
    디스코드 채널로 메시지를 보냅니다.
    """
    if not WEBHOOK_URL:
        logging.error("❌ .env 파일에 디스코드 주소가 없습니다!")
        return

    # 디스코드가 요구하는 데이터 형식 (JSON)
    data = {
        "content": message,
        "username": "AI 투자 비서",  # 보낸 사람 이름 (맘대로 변경 가능)
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/4202/4202831.png"  # 프로필 사진 (로봇 아이콘)
    }

    try:
        response = requests.post(WEBHOOK_URL, json=data)
        if response.status_code == 204:
            logging.info("✅ 디스코드 알림 발송 성공!")
        else:
            logging.error(f"❌ 발송 실패: {response.status_code} - {response.text}")
    except Exception as e:
        logging.error(f"❌ 연결 에러 발생: {e}")


# 테스트 코드 (이 파일을 직접 실행할 때만 작동)
if __name__ == "__main__":
    # 로깅 설정 (눈으로 보기 위해)
    logging.basicConfig(level=logging.INFO)

    print("📡 알림 시스템 테스트 중...")
    send_discord_alert("🚨 주인님! 이것은 테스트 메시지입니다.\n오늘도 엔지니어링 공부 화이팅입니다! 🔥")