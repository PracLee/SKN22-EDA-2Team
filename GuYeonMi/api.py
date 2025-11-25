import requests
import pandas as pd
import math
import time

# ============================================
# 0. 공공데이터포털 서비스 기본 설정
# ============================================
# ▶ 반드시 "일반인증키(디코딩)" 값을 그대로 넣어야 함.
SERVICE_KEY = '8fe35dba7eb16051f50bfff23d255e088fe50c59f1ef59a9e0e2e06fe60e12e8'

# 마약류 생산(수출입)실적 정보 API URL
BASE_URL = "http://apis.data.go.kr/1471000/NarkProdPfmcService/getNarkProdPfmcInq"


# ============================================
# 1. 원시 데이터 수집 함수
# ============================================
def fetch_raw_data(item_name=None, year=None, rows_per_page=500, max_pages=None):
    """
    마약류 생산(수출입)실적 정보를 API에서 전부 가져와서 DataFrame으로 반환.

    - item_name: 품명(부분 일치) 필터. None이면 전체.
      예) item_name="코데날"
    - year: 실적년도(PFMC_YMD). None이면 전체 연도.
      예) year="2023"
    - rows_per_page: 페이지당 레코드 수 (※ API 제약: 최대 500)
    - max_pages: 강제로 제한하고 싶을 때 최대 페이지 수 (None이면 전체)
    """
    # numOfRows는 API 최대 500
    if rows_per_page > 500:
        print(f"[WARN] numOfRows 최대가 500이라서 {rows_per_page} → 500으로 줄입니다.")
        rows_per_page = 500

    all_items = []
    page_no = 1

    while True:
        params = {
            "serviceKey": SERVICE_KEY,
            "type": "json",        # 응답 포맷 JSON
            "pageNo": page_no,
            "numOfRows": rows_per_page,
        }
        if item_name:
            params["ITEM_NAME"] = item_name
        if year:
            params["PFMC_YMD"] = year

        # -------------------------------
        # HTTP 요청
        # -------------------------------
        resp = requests.get(BASE_URL, params=params, timeout=30)
        print(f"[DEBUG] HTTP status = {resp.status_code}, page = {page_no}")

        # HTTP 단계에서 에러 (예: 401/403 Forbidden 등)
        if resp.status_code != 200:
            print("[ERROR] HTTP 상태 코드가 200이 아님. 원문 응답:")
            print(resp.text[:500])
            break

        # -------------------------------
        # JSON 파싱
        # -------------------------------
        try:
            data = resp.json()
        except Exception:
            print("[ERROR] JSON 파싱 실패, 원문 응답:")
            print(resp.text[:500])
            break

        # 공공데이터포털 기본 구조 언랩
        # 보통: { "response": { "header": {...}, "body": {...} } }
        response = data.get("response", data)
        header = response.get("header", {})
        body = response.get("body", response)

        result_code = header.get("resultCode")
        result_msg = header.get("resultMsg")

        # resultCode가 있으면 찍어서 확인 (00이 정상)
        if result_code is not None:
            print(f"[DEBUG] resultCode={result_code}, resultMsg={result_msg}")
            if str(result_code) != "00":
                print("[ERROR] API 에러(resultCode != '00')로 데이터 수집 중단")
                break

        # -------------------------------
        # items 추출
        # -------------------------------
        items = body.get("items")

        # items가 없으면 더 이상 데이터 없거나, 조건에 맞는 데이터 없음
        if not items:
            print("[DEBUG] items가 비어 있음. 데이터가 없거나 필터 조건에 맞는 결과가 없음.")
            break

        # items가 list인지 dict인지 모두 처리
        if isinstance(items, list):
            all_items.extend(items)
        else:
            all_items.append(items)

        # totalCount / numOfRows 추출
        total_count = int(body.get("totalCount", len(all_items)) or 0)
        num_of_rows = int(body.get("numOfRows", rows_per_page) or rows_per_page)

        # 마지막 페이지 계산
        last_page = math.ceil(total_count / num_of_rows) if num_of_rows > 0 else 1
        print(f"[INFO] page {page_no}/{last_page} 수집 완료 (누적 {len(all_items)}건)")

        # 페이지 제한 옵션
        if max_pages and page_no >= max_pages:
            print("[INFO] max_pages에 도달하여 중단")
            break
        if page_no >= last_page:
            print("[INFO] 마지막 페이지까지 수집 완료")
            break

        page_no += 1
        time.sleep(0.2)  # 서버 부담 줄이기 위해 잠깐 대기

    # -------------------------------
    # 최종 DataFrame 변환
    # -------------------------------
    if not all_items:
        print("수집된 데이터가 없습니다.")
        return pd.DataFrame()

    df = pd.DataFrame(all_items)
    return df


# ============================================
# 2. 연도별 합계 집계 함수
# ============================================
def aggregate_by_year(df, output_csv="year_summary.csv"):
    """
    원시 DataFrame(df)을 받아서 연도별 생산·허가량 합계 CSV로 저장.

    - 연도 컬럼: PFMC_YMD
    - 수량 컬럼: PROD_QTY (생산·수출입량), PRMSN_QTY(허가량)
    """
    if df.empty:
        print("[WARN] 입력 DataFrame이 비어 있음. 집계 없이 종료.")
        empty_df = pd.DataFrame(columns=["year", "prod_qty_sum", "prmsn_qty_sum"])
        empty_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
        print(f"[INFO] 빈 연도별 합계 CSV 저장 완료: {output_csv}")
        return empty_df

    # 필요한 컬럼이 없으면 추가 (NaN → 0으로 처리 예정)
    cols_needed = ["PFMC_YMD", "PROD_QTY", "PRMSN_QTY"]
    for c in cols_needed:
        if c not in df.columns:
            df[c] = None

    # 숫자형으로 변환
    df["PFMC_YMD"] = pd.to_numeric(df["PFMC_YMD"], errors="coerce")
    df["PROD_QTY"] = pd.to_numeric(df["PROD_QTY"], errors="coerce").fillna(0)
    df["PRMSN_QTY"] = pd.to_numeric(df["PRMSN_QTY"], errors="coerce").fillna(0)

    # 유효한 연도만 사용
    df = df.dropna(subset=["PFMC_YMD"])
    df["PFMC_YMD"] = df["PFMC_YMD"].astype(int)

    # 연도별 합계
    grouped = (
        df.groupby("PFMC_YMD", as_index=False)[["PROD_QTY", "PRMSN_QTY"]]
        .sum()
        .sort_values("PFMC_YMD")
    )

    grouped.rename(
        columns={
            "PFMC_YMD": "year",
            "PROD_QTY": "prod_qty_sum",
            "PRMSN_QTY": "prmsn_qty_sum",
        },
        inplace=True,
    )

    grouped.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"[INFO] 연도별 합계 CSV 저장 완료: {output_csv}")
    return grouped


# ============================================
# 3. 메인 실행부
# ============================================
if __name__ == "__main__":
    # ----------------------------------------
    # 1) 전체 마약류 데이터 수집
    #    - 특정 품명만 보고 싶으면 item_name="코데날" 이렇게 넣어도 됨.
    #    - 특정 연도만 보고 싶으면 year="2023" 같은 식으로.
    # ----------------------------------------
    df_raw = fetch_raw_data(
        item_name=None,   # 예: "코데날"
        year=None,        # 예: "2023"
        rows_per_page=500,  # ★ 최대 500
        max_pages=None      # 테스트용이면 2~3 정도로 제한 가능
    )

    print(f"[INFO] 원시 데이터 건수: {len(df_raw)}")
    if not df_raw.empty:
        print(df_raw.head())

    # ----------------------------------------
    # 2) 연도별 수량 합산 후 CSV 저장
    # ----------------------------------------
    df_year = aggregate_by_year(df_raw, output_csv="narcotics_year_summary.csv")

    print(df_year.head())
