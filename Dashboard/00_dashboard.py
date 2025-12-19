import sys
import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# --- [1. 설정 및 데이터 준비] ---
# 현재 파일 위치를 기준으로 상위 폴더 경로 추가 (scripts 등을 불러오기 위해)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# 페이지 기본 설정 (제목, 아이콘, 레이아웃)
st.set_page_config(
    page_title="경제 데이터 상황실",
    page_icon="📈",
    layout="wide"
)


# DB 연결 함수 (Streamlit은 캐싱 기능이 있어서, 매번 로딩 안 하고 빠르게 보여줍니다)
@st.cache_data
def load_data(ticker):
    DB_URI = "postgresql+psycopg2://xodh3@localhost:5432/economy_db"
    engine = create_engine(DB_URI)

    query = f"""
    SELECT trade_date, close_price 
    FROM practice_spy 
    WHERE ticker = '{ticker}' 
    ORDER BY trade_date ASC
    """
    df = pd.read_sql(query, engine)
    return df


# --- [2. 웹사이트 화면 구성] ---

# 제목
st.title("📊 나만의 경제 데이터 상황실")
st.markdown("---")  # 가로줄 긋기

# 사이드바 (왼쪽 메뉴)
st.sidebar.header("검색 옵션")
selected_ticker = st.sidebar.selectbox("종목을 선택하세요", ["QQQ", "SPY", "GLD", "TLT"])

# 데이터 불러오기
st.write(f"### 🚀 {selected_ticker} 분석 대시보드")
df = load_data(selected_ticker)

if df.empty:
    st.error("데이터가 없습니다! 수집기를 먼저 실행해주세요.")
else:
    # --- [3. 핵심 지표 보여주기 (Metric)] ---
    # 최신 가격과 전일 대비 등락폭 계산
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    price = latest['close_price']
    diff = price - prev['close_price']
    diff_pct = (diff / prev['close_price']) * 100

    # 멋진 숫자판(Metric) 표시
    col1, col2, col3 = st.columns(3)
    col1.metric("현재 가격", f"${price:.2f}", f"{diff:.2f} ({diff_pct:.2f}%)")
    col2.metric("데이터 기준일", latest['trade_date'].strftime('%Y-%m-%d'))
    col3.metric("보유 데이터 수", f"{len(df)} rows")

    # --- [4. 차트 그리기 (Plotly)] ---
    st.subheader("📈 가격 변동 차트")

    # 20일 이동평균선 추가 계산 (즉석에서!)
    df['MA20'] = df['close_price'].rolling(window=20).mean()

    # 차트 생성
    fig = px.line(df, x='trade_date', y=['close_price', 'MA20'],
                  title=f"{selected_ticker} Price Movement",
                  labels={'value': 'Price', 'trade_date': 'Date'})

    # 웹사이트에 차트 뿌리기
    st.plotly_chart(fig, use_container_width=True)

    # --- [5. 원본 데이터 보기 (선택사항)] ---
    with st.expander("📄 원본 데이터 확인하기"):
        st.dataframe(df.sort_values(by='trade_date', ascending=False))