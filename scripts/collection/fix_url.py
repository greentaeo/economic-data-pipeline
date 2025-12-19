import urllib.parse

# 1. 여기에 Supabase 대시보드에서 본 'Project ID'를 넣으세요.
# (주소 중간에 있는 긴 영문자입니다. 님 로그에 있는 걸 가져왔습니다.)
PROJECT_ID = "ofnbghvsjrruihimtlap"

# 2. 여기에 아까 설정한 '새 비밀번호'를 넣으세요.
PASSWORD = "vefgu9-Budtiz-rokzec"

# ---------------------------------------------------------
# 건드리지 마세요 (자동 변환 로직)
# ---------------------------------------------------------
# 비밀번호 특수문자를 안전하게 변환 (%40, %21 등으로 바꿈)
encoded_password = urllib.parse.quote_plus(PASSWORD)

# 안전한 주소 생성 (Session 모드, 포트 5432)
safe_uri = f"postgresql://postgres.{PROJECT_ID}:{encoded_password}@aws-0-us-west-2.pooler.supabase.com:5432/postgres"

print("\n" + "="*60)
print("✅ .env 파일에 복사해서 넣을 주소는 아래와 같습니다:")
print("="*60)
print(f"SUPABASE_DB_URI=\"{safe_uri}\"")
print("="*60 + "\n")