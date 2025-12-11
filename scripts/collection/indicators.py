# scripts/indicators.py

fred_indicators = {
    'macro': [
        {'id': 'GDP', 'name_kr': '명목 GDP'},
        {'id': 'GDPC1', 'name_kr': '실질 GDP'},
        {'id': 'CPIAUCSL', 'name_kr': '소비자물가지수'},
        {'id': 'CPILFESL', 'name_kr': '근원 소비자물가지수 (추가)'}, # Core CPI 추가
        {'id': 'PCEPI', 'name_kr': 'PCE 물가지수'},
        {'id': 'FEDFUNDS', 'name_kr': '연방기금금리'},
        {'id': 'M2SL', 'name_kr': 'M2 통화량 (추가)'} # M2 통화량 추가
    ],
    'employment': [
        {'id': 'UNRATE', 'name_kr': '실업률'},
        {'id': 'PAYEMS', 'name_kr': '비농업 고용자 수'},
        {'id': 'CIVPART', 'name_kr': '경제활동참가율'},
        {'id': 'ICSA', 'name_kr': '신규 실업수당 청구 건수'}
    ],
    'markets': [
        {'id': 'NASDAQCOM', 'name_kr': '나스닥 종합지수'},
        {'id': 'DGS10', 'name_kr': '10년 국채금리'},
        {'id': 'DGS2', 'name_kr': '2년 국채금리'},
        {'id': 'T10Y2Y', 'name_kr': '장단기 금리차'},
        {'id': 'VIXCLS', 'name_kr': 'VIX 변동성 지수'}
    ],
    'real_estate': [
        {'id': 'HOUST', 'name_kr': '주택착공'},
        {'id': 'PERMIT', 'name_kr': '건축허가'},
        {'id': 'MORTGAGE30US', 'name_kr': '30년 모기지 금리'},
        {'id': 'MSPUS', 'name_kr': '중간주택가격'},
        {'id': 'CSUSHPISA', 'name_kr': '케이스-쉴러 주택가격지수'}
    ],
    'commodities': [
        {'id': 'DCOILWTICO', 'name_kr': 'WTI 원유 가격'},
        {'id': 'DCOILBRENTEU', 'name_kr': '브렌트유 가격'},
        {'id': 'PCOPPUSDM', 'name_kr': '구리 가격'},
        {'id': 'GOLDPMGBD228NLBM', 'name_kr': '금 가격'}
    ],
    'industrials': [
        {'id': 'INDPRO', 'name_kr': '산업생산지수'},
        {'id': 'NAPM', 'name_kr': 'ISM 제조업 지수 (추가)'}, # ISM 제조업 추가
        {'id': 'NEWORDER', 'name_kr': '제조업 신규주문'},
        {'id': 'CAPUTLB50001SQ', 'name_kr': '설비가동률'}
    ],
    'consumer': [
        {'id': 'RSAFS', 'name_kr': '소매판매'},
        {'id': 'PCE', 'name_kr': '개인소비지출'},
        {'id': 'PSAVERT', 'name_kr': '개인저축률'},
        {'id': 'UMCSENT', 'name_kr': '소비자신뢰지수'}
    ],
    'international': [
        {'id': 'BOPGSTB', 'name_kr': '무역수지'},
        {'id': 'DEXUSEU', 'name_kr': '달러/유로 환율'},
        {'id': 'DEXCHUS', 'name_kr': '달러/위안 환율'},
        {'id': 'DTWEXBGS', 'name_kr': '달러 인덱스'}
    ]
}