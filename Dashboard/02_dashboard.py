import sys
import os
import time
import streamlit as st
import pandas as pd
import plotly.graph_objects as go  # 캔들스틱용 고급 차트 도구
from sqlalchemy import create_engine
from dotenv import load_dotenv


# --- [1. 설정 및 데이터 준비] ---
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

st.set_page_config(
    page_title="경제 데이터 상황실 v2.0",
    page_icon="📊",
    layout="wide"
)

# 자동 새로고침 (Auto Refresh) - 60초마다
if 'last_updated' not in st.session_state:
    st.session_state.last_updated = time.time()


# [수정할 부분: load_data 함수]

@st.cache_data(ttl=60)
def load_data(ticker):
    # 1. 클라우드(Streamlit) 비밀 금고에 주소가 있나? (배포 환경)
    if "SUPABASE_DB_URI" in st.secrets:
        DB_URI = st.secrets["SUPABASE_DB_URI"]
    else:
        # 2. 없으면 내 맥북 .env 파일에서 찾자 (로컬 개발 환경)
        load_dotenv()
        DB_URI = os.getenv("SUPABASE_DB_URI")

    # 3. 그래도 없으면 에러 내지 말고 기본 로컬 주소 (비상용)
    if not DB_URI:
        DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"

    # DB 연결
    engine = create_engine(DB_URI)

    query = f"""
    SELECT trade_date, open_price, high_price, low_price, close_price, volume
    FROM practice_spy 
    WHERE ticker = '{ticker}' 
    ORDER BY trade_date ASC
    """
    df = pd.read_sql(query, engine)
    return df

# --- [2. 사이드바 메뉴] ---
st.sidebar.title("🎛️ 제어 패널")
selected_ticker = st.sidebar.selectbox("종목 선택", ["QQQ", "SPY", "GLD", "TLT"])
refresh_rate = st.sidebar.slider("새로고침 주기 (초)", 10, 300, 60)

if st.sidebar.button("🔄 수동 새로고침"):
    st.cache_data.clear()
    st.rerun()

# --- [3. 메인 화면 구성] ---
st.title(f"📊 {selected_ticker} 실시간 분석 상황실")
st.markdown(f"마지막 업데이트: {time.strftime('%H:%M:%S')}")

df = load_data(selected_ticker)

if df.empty:
    st.error("데이터가 없습니다! 수집기를 먼저 실행해주세요.")
else:
    # [핵심 지표 4개 배치]
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    diff = latest['close_price'] - prev['close_price']
    diff_pct = (diff / prev['close_price']) * 100
    color = "normal" if diff >= 0 else "inverse"  # 오르면 초록/빨강, 내리면 반대

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("현재가 (Close)", f"${latest['close_price']:.2f}", f"{diff:.2f} ({diff_pct:.2f}%)")
    col2.metric("시가 (Open)", f"${latest['open_price']:.2f}")
    col3.metric("고가 (High)", f"${latest['high_price']:.2f}")
    col4.metric("거래량 (Volume)", f"{latest['volume']:,}")

    # --- [4. 캔들스틱 차트 그리기] ---
    st.subheader("🕯️ 가격 변동 (Candlestick Chart)")

    # 이동평균선 계산
    df['MA5'] = df['close_price'].rolling(window=5).mean()
    df['MA20'] = df['close_price'].rolling(window=20).mean()

    # 복합 차트 (캔들 + 이평선)
    fig = go.Figure()

    # 1. 캔들스틱 (봉 차트)
    fig.add_trace(go.Candlestick(
        x=df['trade_date'],
        open=df['open_price'], high=df['high_price'],
        low=df['low_price'], close=df['close_price'],
        name='OHLC'
    ))

    # 2. 이동평균선 (선 차트)
    fig.add_trace(go.Scatter(x=df['trade_date'], y=df['MA5'], line=dict(color='orange', width=1), name='MA 5일선'))
    fig.add_trace(go.Scatter(x=df['trade_date'], y=df['MA20'], line=dict(color='blue', width=1), name='MA 20일선'))

    # 차트 꾸미기 (줌 슬라이더 제거 등)
    fig.update_layout(
        xaxis_rangeslider_visible=False,  # 밑에 지저분한 슬라이더 끄기
        height=600,
        title=f"{selected_ticker} Daily Chart",
        yaxis_title="Price ($)"
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- [5. 데이터 테이블 (숨김 기능)] ---
    with st.expander("📋 상세 데이터 보기 (최근 10일)"):
        st.dataframe(df.sort_values(by='trade_date', ascending=False).head(10))

# 자동 새로고침 로직
time.sleep(refresh_rate)
st.rerun()