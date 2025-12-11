import os
import shutil
import glob

# --- [설정] 경로 지정 ---
BASE_DIR = "data/01_raw"
DATA_DIR = "data/DATA"  # 섞여 있는 폴더

# 1. 목적지 (이사 갈 방)
TARGET_PRICE = os.path.join(BASE_DIR, "market_price")
TARGET_MACRO = os.path.join(BASE_DIR, "macro_series")
TARGET_META = os.path.join(BASE_DIR, "metadata")

# 2. 소스 (이사 올 짐들) - 폴더 이름들
# (1) 가격 데이터(OHLC)로 분류할 폴더들
PRICE_FOLDERS = ["etfs", "forex", "markets"]

# (2) 거시경제(Macro)로 분류할 폴더들
MACRO_FOLDERS = [
    "commodities", "consumer", "employment",
    "industrials", "international", "macro",
    "real_estate", "fred_indicators"  # 여기는 하위 폴더가 많음 (특수 처리)
]


def move_files(src_folder, target_folder, is_flatten=False):
    """
    폴더 안의 CSV 파일들을 목적지로 이동시킵니다.
    is_flatten=True면 하위 폴더에 있는 것까지 다 꺼내옵니다.
    """
    src_path = os.path.join(BASE_DIR, src_folder)

    if not os.path.exists(src_path):
        print(f"⚠️ 폴더 없음(건너뜀): {src_path}")
        return

    print(f"📦 [{src_folder}] 정리 중... -> {target_folder}")

    # 하위 폴더까지 뒤질 것인가?
    if is_flatten:
        # 예: fred_indicators/**/(*.csv)
        files = glob.glob(os.path.join(src_path, "**/*.csv"), recursive=True)
    else:
        # 예: etfs/*.csv
        files = glob.glob(os.path.join(src_path, "*.csv"))

    count = 0
    for file_path in files:
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(target_folder, file_name)

        try:
            # 파일 이동 (이미 있으면 덮어쓰기 or 건너뛰기 고민 필요하지만 일단 이동)
            if os.path.exists(dest_path):
                print(f"   ⚠️ 중복 파일 발견 (건너뜀): {file_name}")
            else:
                shutil.move(file_path, dest_path)
                count += 1
        except Exception as e:
            print(f"   ❌ 이동 실패: {file_name} / {e}")

    print(f"   👉 {count}개 파일 이동 완료.\n")


def sort_mixed_data_folder():
    """
    data/DATA 폴더에 섞여 있는 파일들을 이름 보고 분류해서 이동
    """
    if not os.path.exists(DATA_DIR):
        return

    print(f"📦 [DATA (혼합 폴더)] 정리 중...")
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    for file_path in files:
        filename = os.path.basename(file_path)
        dest = None

        # 1. 메타데이터 (설명서)
        if "country_United_States" in filename:
            dest = TARGET_META

        # 2. 가격 데이터 (markets_historical_...)
        elif filename.startswith("markets_historical") or "usdkrw" in filename:
            dest = TARGET_PRICE

        # 3. 나머지는 다 경제 지표 (historical_country_...)
        elif filename.startswith("historical_country") or "commodities" in filename:
            dest = TARGET_MACRO

        if dest:
            try:
                shutil.move(file_path, os.path.join(dest, filename))
                print(f"   ✅ 이동: {filename} -> {dest}")
            except:
                pass
    print("   👉 DATA 폴더 정리 끝.\n")


# --- 실행 ---
if __name__ == "__main__":
    print("🚀 데이터 대청소 시작!\n")

    # 1. 가격 데이터 폴더 이동
    for folder in PRICE_FOLDERS:
        move_files(folder, TARGET_PRICE)

    # 2. 경제 지표 폴더 이동 (fred_indicators는 flatten=True)
    for folder in MACRO_FOLDERS:
        is_flat = (folder == "fred_indicators")
        move_files(folder, TARGET_MACRO, is_flatten=is_flat)

    # 3. DATA 폴더(혼합) 정리
    sort_mixed_data_folder()

    print("🎉 정리 완료! 'tree data/01_raw' 명령어로 확인해보세요.")