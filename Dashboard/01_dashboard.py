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

# 파일 위치를 정확하게 명시합니다.
DOTENV_PATH = os.path.join(os.path.dirname(__file__), '..', '.env') # Dashboard/ -> Project Root
if os.path.exists(DOTENV_PATH):
    load_dotenv(DOTENV_PATH)

st.set_page_config(
    page_title="경제 데이터 상황실 v2.0",
    page_icon="📊",
    layout="wide"
)

# 자동 새로고침 (Auto Refresh) - 60초마다
if 'last_updated' not in st.session_state:
    st.session_state.last_updated = time.time()


# [수정할 부분: load_data 함수]

# [수정할 부분: load_data 함수]

@st.cache_data(ttl=60)
def load_data(ticker):
    DB_URI = None

    # 1. 로컬 환경 변수 (.env)에서 먼저 가져옵니다. (가장 확실한 방법)
    # load_dotenv는 상단에서 이미 호출했으므로 os.getenv로 바로 접근합니다.
    DB_URI = os.getenv("SUPABASE_DB_URI")
    # 1. Streamlit Cloud 환경인지 확인하고 st.secrets에서 가져옵니다.
    try:
        # 로컬에서 에러가 날 수 있는 st.secrets 접근을 try로 감쌉니다.
        if "SUPABASE_DB_URI" in st.secrets:
            DB_URI = st.secrets["SUPABASE_DB_URI"]
    except:
        # 2. 로컬 환경 변수에서 가져옵니다. (load_dotenv로 이미 로드됨)
        DB_URI = os.getenv("SUPABASE_DB_URI")

    # 3. 그래도 없으면 에러 내지 말고 기본 로컬 주소 (비상용)
    if not DB_URI:
        # 이 주소는 님이 로컬에서 PostgreSQL을 돌릴 때 쓰는 주소입니다.
        # 이 주소도 작동하지 않으면, 님의 .env 파일에 문제가 있을 가능성이 큽니다.
        DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"
        # st.error("경고: .env 파일에서 DB 주소를 찾지 못했습니다. 비상용 로컬 주소를 사용합니다.")

        # DB 연결
    engine = create_engine(DB_URI)

    query = f"""
        SELECT trade_date, open_price, high_price, low_price, close_price, volume
        FROM market_price_daily  -- 최종 테이블 이름
        WHERE symbol = '{ticker}' -- 최종 컬럼 이름
        ORDER BY trade_date ASC
        """
    df = pd.read_sql(query, engine)
    return df


# ... (나머지 코드는 그대로)

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
    # --- 👇👇👇 핵심 지표 4개 배치 (안전한 포맷팅 적용) 👇👇👇 ---

    # 1. 'close_price'가 있는 유효한 행만 필터링합니다. (가장 중요한 데이터만 필터링)
    df_valid = df.dropna(subset=['close_price', 'trade_date']).copy()

    if df_valid.empty:
        st.error("선택된 종목의 유효한 종가(Close Price) 데이터가 없습니다. DB 로드/수집을 확인해주세요.")
    else:
        # 2. 가장 최근의 유효한 데이터 (Latest)
        latest = df_valid.iloc[-1]

        # 3. 그 직전의 데이터 (Previous) 및 변동성 계산
        if len(df_valid) >= 2:
            prev = df_valid.iloc[-2]
            diff = latest['close_price'] - prev['close_price']
            diff_pct = (diff / prev['close_price']) * 100
        else:
            diff = 0.0
            diff_pct = 0.0

        color = "normal" if diff >= 0 else "inverse"


        # 안전한 포맷팅을 위한 헬퍼 함수
        def safe_format(value, fmt, prefix=''):
            # None 또는 NaN인 경우 'N/A' 반환
            if pd.isna(value) or value is None:
                return "N/A"
            # value가 문자열인 경우도 고려하여 숫자 포맷팅을 시도합니다.
            try:
                return f"{prefix}{value:{fmt}}"
            except:
                return str(value)


        col1, col2, col3, col4 = st.columns(4)

        # Current Price (Close)
        diff_display = f"{diff:.2f} ({diff_pct:.2f}%)" if diff != 0.0 else "0.00 (0.00%)"

        # 👇👇👇 모든 Metric에 safe_format 적용 👇👇👇

        col1.metric("현재가 (Close)", safe_format(latest['close_price'], '.2f', '$'), diff_display)

        # Open
        col2.metric("시가 (Open)", safe_format(latest['open_price'], '.2f', '$'))

        # High
        col3.metric("고가 (High)", safe_format(latest['high_price'], '.2f', '$'))

        # Volume
        col4.metric("거래량 (Volume)", safe_format(latest['volume'], ',.0f'))

        # --- [4. 캔들스틱 차트 그리기] ---
        st.subheader("🕯️ 가격 변동 (Candlestick Chart)")

        # 캔들스틱 차트를 위해 OHLCV 5개 컬럼이 모두 있는 데이터만 필터링
        df_chart = df_valid.dropna(subset=['open_price', 'high_price', 'low_price', 'close_price']).copy()

        if df_chart.empty:
            st.warning("캔들스틱 차트를 그릴 충분한 OHLC 데이터가 없습니다. 종가(Close)만 표시합니다.")
            # 종가만 그리는 라인 차트로 대체
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(x=df_valid['trade_date'], y=df_valid['close_price'], line=dict(color='red', width=2),
                           name='Close Price'))
        else:
            # 이동평균선 계산 (df_chart 사용)
            df_chart['MA5'] = df_chart['close_price'].rolling(window=5).mean()
            df_chart['MA20'] = df_chart['close_price'].rolling(window=20).mean()

            # 복합 차트 (캔들 + 이평선)
            fig = go.Figure()

            # 1. 캔들스틱 (봉 차트)
            fig.add_trace(go.Candlestick(
                x=df_chart['trade_date'],
                open=df_chart['open_price'], high=df_chart['high_price'],
                low=df_chart['low_price'], close=df_chart['close_price'],
                name='OHLC'
            ))

            # 2. 이동평균선 (선 차트)
            fig.add_trace(go.Scatter(x=df_chart['trade_date'], y=df_chart['MA5'], line=dict(color='orange', width=1),
                                     name='MA 5일선'))
            fig.add_trace(go.Scatter(x=df_chart['trade_date'], y=df_chart['MA20'], line=dict(color='blue', width=1),
                                     name='MA 20일선'))

        # 차트 꾸미기 (공통)
        fig.update_layout(
            xaxis_rangeslider_visible=False,
            height=600,
            title=f"{selected_ticker} Daily Chart",
            yaxis_title="Price ($)"
        )

        st.plotly_chart(fig, use_container_width=True)

        # --- [5. 데이터 테이블 (숨김 기능)] ---
        with st.expander("📋 상세 데이터 보기 (최근 10일)"):
            st.dataframe(df_valid.sort_values(by='trade_date', ascending=False).head(10))

# 자동 새로고침 로직
time.sleep(refresh_rate)
st.rerun()