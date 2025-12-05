import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import logging


# --- 데이터 정제 함수 (이전 스크립트와 동일) ---
def clean_numeric_value(value):
    if isinstance(value, str):
        value = value.strip()
        if not value: return np.nan
        multipliers = {'K': 1e3, 'M': 1e6, 'B': 1e9, 'T': 1e12}
        if '%' in value: return float(value.replace('%', ''))
        for char, multiplier in multipliers.items():
            if char in value: return float(value.replace(char, '')) * multiplier
        try:
            return float(value)
        except ValueError:
            return np.nan
    return value


# --- 데이터 준비 함수 (이전 스크립트와 동일) ---
def prepare_surprise_data(calendar_path, market_data_path):
    calendar_df = pd.read_csv(calendar_path, parse_dates=['date'])
    for col in ['actual', 'forecast', 'previous']:
        calendar_df[col] = calendar_df[col].apply(clean_numeric_value)
    calendar_df['surprise'] = calendar_df['actual'] - calendar_df['forecast']

    market_df = pd.read_csv(market_data_path, index_col='Date', parse_dates=True)
    market_df['daily_return'] = market_df['Adj Close'].pct_change() * 100

    final_df = pd.merge(calendar_df, market_df[['daily_return']], left_on='date', right_index=True, how='left')
    final_df = final_df[['date', 'event', 'surprise', 'daily_return']].dropna()
    return final_df


# --- 메인 분석 로직 ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    CALENDAR_FILE_PATH = "data/processed/forex_factory_calendar.csv"
    MARKET_DATA_PATH = "data/raw/etfs/SPY.csv"

    try:
        # 1. 데이터 준비
        analysis_df = prepare_surprise_data(CALENDAR_FILE_PATH, MARKET_DATA_PATH)
        logging.info("분석용 데이터 준비 완료.")

        # 2. '서프라이즈' 종류 분류
        analysis_df['surprise_type'] = 'No Surprise'
        analysis_df.loc[analysis_df['surprise'] > 0, 'surprise_type'] = 'Positive Surprise'
        analysis_df.loc[analysis_df['surprise'] < 0, 'surprise_type'] = 'Negative Surprise'

        # 3. 종류별 평균 수익률 계산
        avg_returns = analysis_df.groupby('surprise_type')['daily_return'].mean().reset_index()
        logging.info("서프라이즈 종류별 평균 수익률 계산 완료.")
        print(avg_returns)

        # 4. 결과 시각화
        plt.figure(figsize=(10, 6))
        sns.barplot(x='surprise_type', y='daily_return', data=avg_returns,
                    order=['Positive Surprise', 'No Surprise', 'Negative Surprise'])

        plt.title('Average SPY Return by Economic Surprise Type', fontsize=16)
        plt.xlabel('Surprise Type', fontsize=12)
        plt.ylabel('Average Daily Return (%)', fontsize=12)
        plt.axhline(0, color='grey', linewidth=0.8)  # 0% 기준선 추가
        plt.show()

    except FileNotFoundError as e:
        logging.error(f"파일을 찾을 수 없습니다: {e.filename}. 데이터 수집이 완료되었는지, 파일 경로가 올바른지 확인해주세요.")