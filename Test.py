import requests
import pandas as pd
from io import BytesIO
from datetime import datetime
import json
import re

# ===========================
# 账号信息
# ===========================
USERNAME = "huming"
PASSWORD = "123456"

# 按你最新要求：inStore 和 orders 都用 p1 token
TOKEN_URL_P1 = "https://api.cbxbvu474n-zhejiangd1-p1-public.model-t.ccv2prod.sapcloud.cn/authorizationserver/oauth/token"

ORDERS_URL = "https://api.cbxbvu474n-zhejiangd1-p1-public.model-t.ccv2prod.sapcloud.cn/xjawebservices/v2/xjab2b/orders"
INSTORE_URL = "https://api.cbxbvu474n-zhejiangd1-p1-public.model-t.ccv2prod.sapcloud.cn/xjawebservices/v2/xjab2b/products/inStore"

ORDERLIST_PUBLIC_DL = "https://regentrv-my.sharepoint.com/:x:/g/personal/planning_regentrv_com_au/ETevaCJOE_ZLqQt1ZH4mcUkBm_zrJBIN5TrKkx6tRn-7_w?e=cff2ie&download=1"
BP_PUBLIC_DL = "https://regentrv-my.sharepoint.com/:x:/g/personal/planning_regentrv_com_au/EUrF3Epmmb1FjIJBCSe0VU0BvMGBfkMnwT2ZvqDKYj7pSA?e=5D4UJq&download=1"

PAGE_SIZE = 200
TIMEOUT = 30

START_DATE = None
END_DATE = None
STATUSES = None


# ===========================
# 通用函数
# ===========================
def clean_chassis(v):
    if pd.isna(v):
        return None
    s = str(v).strip().upper()
    s = s.replace("-", "")
    s = re.sub(r"\s+", "", s)
    return s if s else None


def normalize_code(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    s = s.lstrip("0")
    return s if s else "0"


def delivery_to_text(v):
    if pd.isna(v):
        return None
    try:
        return str(int(float(v)))
    except Exception:
        s = str(v).strip()
        s = s.lstrip("0")
        return s if s else "0"


def mark_match(a, b):
    if pd.isna(a) and pd.isna(b):
        return ""
    if pd.isna(a) or pd.isna(b):
        return "✗"
    return "✓" if str(a).strip() == str(b).strip() else "✗"


def get_nested(d, path, default=None):
    cur = d
    for key in path.split("."):
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def download_file_bytes(session, url):
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.content


def read_excel_bytes_select_sheet(file_bytes, sheet_name):
    xls = pd.ExcelFile(BytesIO(file_bytes))
    if sheet_name not in xls.sheet_names:
        raise ValueError(f"Sheet 不存在: {sheet_name}，现有 sheet: {xls.sheet_names}")
    return pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name)


def normalize_api_value(v):
    """
    最稳的字段标准化：
    1. None / NaN -> None
    2. dict -> 优先提取常见字段；否则转成 JSON 字符串
    3. list -> 转成 JSON 字符串
    4. 空字符串 / 'null' / 'none' / 'nan' -> None
    5. 其他统一转字符串并去空格
    """
    if pd.isna(v):
        return None

    if isinstance(v, dict):
        for k in ["code", "value", "id", "name", "erpPO", "erpSO", "erpPONumber"]:
            if k in v and pd.notna(v.get(k)):
                s = str(v.get(k)).strip()
                if s and s.lower() not in ["null", "none", "nan"]:
                    return s
        try:
            s = json.dumps(v, ensure_ascii=False)
            return s.strip() if s.strip() else None
        except Exception:
            s = str(v).strip()
            return s if s else None

    if isinstance(v, list):
        try:
            s = json.dumps(v, ensure_ascii=False)
            return s.strip() if s.strip() else None
        except Exception:
            s = str(v).strip()
            return s if s else None

    s = str(v).strip()
    if s == "" or s.lower() in ["null", "none", "nan"]:
        return None

    return s


# ===========================
# Token
# ===========================
def get_token(session, token_url, username):
    data = {
        "client_id": "channel",
        "client_secret": "23bc7b32-ef87-4587-8b32-50cd218698a8",
        "grant_type": "password",
        "username": username,
        "password": PASSWORD
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    response = session.post(token_url, data=data, headers=headers, timeout=TIMEOUT)

    print("获取 Token 状态码:", response.status_code, "|", token_url)

    if response.status_code != 200:
        print("❌ 获取 token 失败")
        print(response.text)
        return None

    try:
        token = response.json().get("access_token")
    except Exception as e:
        print("❌ token 解析失败:", str(e))
        print(response.text)
        return None

    if not token:
        print("❌ 没有 access_token")
        return None

    print("✅ Token 获取成功")
    return token


# ===========================
# orders 读取逻辑
# ===========================
def fetch_orders_page(session, token, current_page, page_size):
    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "fields": "DEFAULT",
        "currentPage": current_page,
        "pageSize": page_size
    }

    if START_DATE:
        params["startDate"] = START_DATE
    if END_DATE:
        params["endDate"] = END_DATE
    if STATUSES:
        params["statuses"] = STATUSES

    response = session.get(ORDERS_URL, headers=headers, params=params, timeout=TIMEOUT)

    if response.status_code != 200:
        print(f"❌ 第 {current_page} 页读取失败，状态码:", response.status_code)
        print(response.text[:1000])
        return None

    try:
        return response.json()
    except Exception as e:
        print(f"❌ 第 {current_page} 页 JSON 解析失败:", str(e))
        print(response.text[:1000])
        return None


def extract_order_list(data):
    if not isinstance(data, dict):
        return []

    candidate_keys = [
        "results",
        "orders",
        "orderList",
        "entries",
        "data",
        "items",
        "list"
    ]

    for key in candidate_keys:
        value = data.get(key)
        if isinstance(value, list):
            return value

    for key, value in data.items():
        if isinstance(value, dict):
            for subkey in candidate_keys:
                subvalue = value.get(subkey)
                if isinstance(subvalue, list):
                    return subvalue

    return []


def fetch_all_orders(session, token):
    all_data = []
    current_page = 0

    while True:
        data = fetch_orders_page(session, token, current_page=current_page, page_size=PAGE_SIZE)
        if not data:
            print(f"⚠️ 第 {current_page} 页无有效响应，停止分页")
            break

        page_results = extract_order_list(data)

        if current_page == 0:
            print("\n========== 第 0 页返回结构检查 ==========")
            print("顶层 keys:", list(data.keys()))
            print(json.dumps(data, ensure_ascii=False, indent=2)[:3000])

            pagination = data.get("pagination", {})
            total_pages = pagination.get("totalPages")
            total_count = pagination.get("totalCount")
            if total_pages is not None:
                print("\n总页数(接口返回):", total_pages)
            if total_count is not None:
                print("总条数(接口返回):", total_count)

        print(f"第 {current_page} 页条数:", len(page_results))

        if not page_results:
            print("✅ 已到最后一页")
            break

        for r in page_results:
            r["pageNumber"] = current_page
        all_data.extend(page_results)

        current_page += 1

        # 防止接口异常导致死循环
        if current_page > 1000:
            print("⚠️ 超过1000页，强制停止（防死循环）")
            break

    print("✅ orders 最终读取条数:", len(all_data))
    return all_data


# ===========================
# inStore 读取逻辑
# ===========================
def fetch_instore_page(session, token, current_page, page_size):
    headers = {
        "Authorization": f"Bearer {token}"
    }

    params = {
        "fields": "DEFAULT",
        "currentPage": current_page,
        "pageSize": page_size
    }

    response = session.get(INSTORE_URL, headers=headers, params=params, timeout=TIMEOUT)

    if response.status_code != 200:
        print(f"❌ inStore 第 {current_page} 页失败，状态码:", response.status_code)
        print(response.text[:1000])
        return None

    try:
        return response.json()
    except Exception as e:
        print(f"❌ inStore 第 {current_page} 页 JSON 解析失败:", str(e))
        print(response.text[:1000])
        return None


def fetch_all_instore(session, token):
    all_data = []

    first_data = fetch_instore_page(session, token, current_page=0, page_size=PAGE_SIZE)
    if not first_data:
        return []

    pagination = first_data.get("pagination", {})
    results = first_data.get("results", [])

    total_pages = pagination.get("totalPages", 1)
    print("\ninStore 总页数:", total_pages)
    print("inStore 第 0 页条数:", len(results))

    all_data.extend(results)

    for current_page in range(1, total_pages):
        data = fetch_instore_page(session, token, current_page=current_page, page_size=PAGE_SIZE)
        if not data:
            break

        page_results = data.get("results", [])
        print(f"inStore 第 {current_page} 页条数:", len(page_results))

        if not page_results:
            break

        all_data.extend(page_results)

    print("✅ inStore 最终读取条数:", len(all_data))
    return all_data


# ===========================
# 主报表逻辑
# ===========================
def build_report():
    session = requests.Session()

    # 按你最新要求：两个接口都用 p1 token
    token_p1 = get_token(session, TOKEN_URL_P1, USERNAME)
    if not token_p1:
        raise ValueError("token 获取失败")

    # 读取 API
    orders_rows = fetch_all_orders(session, token_p1)
    instore_rows = fetch_all_instore(session, token_p1)

    # 下载 Excel
    orderlist_bytes = download_file_bytes(session, ORDERLIST_PUBLIC_DL)
    bp_bytes = download_file_bytes(session, BP_PUBLIC_DL)

    # ===========================
    # Orderlist
    # ===========================
    ol = read_excel_bytes_select_sheet(orderlist_bytes, "Orderlist")
    for c in ["Chassis", "Regent Production", "Model", "Dealer", "Customer"]:
        if c not in ol.columns:
            ol[c] = None

    ol["Chassis_Clean"] = ol["Chassis"].apply(clean_chassis)
    ol = ol[ol["Regent Production"].fillna("").astype(str).str.strip() != "Finished"].copy()

    ol_dedup = (
        ol[["Chassis", "Chassis_Clean", "Regent Production", "Model", "Dealer", "Customer"]]
        .dropna(subset=["Chassis_Clean"])
        .drop_duplicates(subset=["Chassis_Clean"], keep="first")
        .copy()
    )

    # ===========================
    # BP
    # ===========================
    bp = read_excel_bytes_select_sheet(bp_bytes, "BP")
    for c in ["Abbrev.", "Delivery to (SAP Code)"]:
        if c not in bp.columns:
            bp[c] = None

    bp["DeliveryTo_Text"] = bp["Delivery to (SAP Code)"].apply(delivery_to_text)

    bp_dedup = (
        bp[["Abbrev.", "DeliveryTo_Text"]]
        .drop_duplicates(subset=["Abbrev."], keep="first")
        .copy()
    )

    # ===========================
    # inStore 缺失 erpSO（按最新要求）
    # ===========================
    instore_df = pd.json_normalize(instore_rows)

    for c in ["code", "erpPO", "erpSO", "dealer", "soldTo", "stockStatusCode"]:
        if c not in instore_df.columns:
            instore_df[c] = None

    instore_df["erpPO_norm"] = instore_df["erpPO"].apply(normalize_api_value)
    instore_df["erpSO_norm"] = instore_df["erpSO"].apply(normalize_api_value)

    instore_df["erpPO_missing"] = instore_df["erpPO_norm"].apply(lambda x: "✗" if x is None else "✓")
    instore_df["erpSO_missing"] = instore_df["erpSO_norm"].apply(lambda x: "✗" if x is None else "✓")

    instore_error = instore_df[
        instore_df["erpSO_norm"].isna()
    ].copy()

    instore_error["error_type"] = "inStore 缺失 erpSO"

    instore_error_final = instore_error[
        [
            "code",
            "dealer",
            "soldTo",
            "stockStatusCode",
            "erpSO",
            "erpSO_norm",
            "erpSO_missing",
            "error_type"
        ]
    ].copy()

    # ===========================
    # orders 处理
    # 增加 erpPONumber 检查
    # ===========================
    orders_main = []
    for row in orders_rows:
        erp_po_number = row.get("erpPONumber")

        orders_main.append({
            "code": row.get("code"),
            "carFrameNumber": row.get("carFrameNumber"),
            "carFrameNumber_Clean": clean_chassis(row.get("carFrameNumber")),
            "dealerCode": normalize_code(row.get("dealerCode")),
            "erpSONumber": row.get("erpSONumber"),
            "erpPONumber": erp_po_number,
            "erpPONumber_norm": normalize_api_value(erp_po_number),
            "orgCustomer.orgUnit.uid": normalize_code(get_nested(row, "orgCustomer.orgUnit.uid")),
            "dealerName": row.get("dealerName"),
            "customer": row.get("customer"),
            "status": row.get("status"),
            "statusDisplay": row.get("statusDisplay")
        })

    main_df = pd.DataFrame(orders_main)

    if main_df.empty:
        main_df = pd.DataFrame(columns=[
            "code", "carFrameNumber", "carFrameNumber_Clean", "dealerCode",
            "erpSONumber", "erpPONumber", "erpPONumber_norm",
            "orgCustomer.orgUnit.uid", "dealerName", "customer",
            "status", "statusDisplay"
        ])

    main_df["erpPONumber_missing"] = main_df["erpPONumber_norm"].apply(lambda x: "✗" if x is None else "✓")

    main_df = main_df[main_df["carFrameNumber_Clean"].notna()].copy()
    main_dedup = main_df.drop_duplicates(subset=["carFrameNumber_Clean"], keep="first").copy()

    # 只保留能匹配到 Orderlist 的
    merged = main_dedup.merge(
        ol_dedup[["Chassis", "Chassis_Clean", "Regent Production", "Model", "Dealer", "Customer"]],
        how="inner",
        left_on="carFrameNumber_Clean",
        right_on="Chassis_Clean"
    )

    # 用 Dealer 去 BP.Abbrev. 找 SAP Code
    merged = merged.merge(
        bp_dedup,
        how="left",
        left_on="Dealer",
        right_on="Abbrev."
    )

    merged["Expected_SAP_Code"] = merged["DeliveryTo_Text"].apply(normalize_code)

    merged["dealerCode_Check"] = merged.apply(
        lambda r: mark_match(r["dealerCode"], r["Expected_SAP_Code"]), axis=1
    )

    merged["orgUnitUid_Check"] = merged.apply(
        lambda r: mark_match(r["orgCustomer.orgUnit.uid"], r["Expected_SAP_Code"]), axis=1
    )

    # orders 错误条件：
    # 1. dealerCode 不匹配
    # 2. orgCustomer.orgUnit.uid 不匹配
    # 3. erpPONumber 缺失
    final_error = merged[
        (merged["dealerCode_Check"] == "✗") |
        (merged["orgUnitUid_Check"] == "✗") |
        (merged["erpPONumber_missing"] == "✗")
    ].copy()

    final_error["error_type"] = final_error.apply(
        lambda r: "; ".join([
            x for x in [
                "dealerCode 校验异常" if r["dealerCode_Check"] == "✗" else None,
                "orgCustomer.orgUnit.uid 校验异常" if r["orgUnitUid_Check"] == "✗" else None,
                "erpPONumber 缺失" if r["erpPONumber_missing"] == "✗" else None
            ] if x
        ]),
        axis=1
    )

    if "Regent Production" not in final_error.columns:
        final_error["Regent Production"] = None

    final_error_report = final_error[
        [
            "carFrameNumber",
            "dealerCode",
            "erpSONumber",
            "erpPONumber",
            "erpPONumber_norm",
            "erpPONumber_missing",
            "orgCustomer.orgUnit.uid",
            "Chassis",
            "Regent Production",
            "Dealer",
            "Expected_SAP_Code",
            "dealerCode_Check",
            "orgUnitUid_Check",
            "error_type"
        ]
    ].copy()

    # ===========================
    # NEW: orders_po_missing 新逻辑（按你要求）
    # ===========================
    target_dealers = ["Geelong", "ST James", "Launceston", "Traralgon", "Frankston"]

    # 1. 过滤 Orderlist
    ol_target = ol_dedup[
        ol_dedup["Dealer"].isin(target_dealers)
    ].copy()

    # 2. 准备 orders API 数据（按 chassis 建索引）
    orders_map = main_dedup.set_index("carFrameNumber_Clean")

    # 3. 准备 instore API 数据（按 code/chassis 建索引）
    instore_df["code_clean"] = instore_df["code"].apply(clean_chassis)
    instore_map = instore_df.set_index("code_clean")

    results = []

    for _, row in ol_target.iterrows():
        chassis = row["Chassis_Clean"]

        orders_row = orders_map.loc[chassis] if chassis in orders_map.index else None
        instore_row = instore_map.loc[chassis] if chassis in instore_map.index else None

        erpSO = None
        erpPO = None

        # 从 orders 取
        if orders_row is not None:
            erpSO = orders_row.get("erpSONumber")
            erpPO = orders_row.get("erpPONumber")

        # 如果 orders 没有，再从 instore 兜底
        if (erpSO is None or str(erpSO).strip() == "") and instore_row is not None:
            erpSO = instore_row.get("erpSO")

        if (erpPO is None or str(erpPO).strip() == "") and instore_row is not None:
            erpPO = instore_row.get("erpPO")

        erpSO_norm = normalize_api_value(erpSO)
        erpPO_norm = normalize_api_value(erpPO)

        erpSO_missing = "✗" if erpSO_norm is None else "✓"
        erpPO_missing = "✗" if erpPO_norm is None else "✓"

        # ❗ 核心规则：只要一个缺就报错
        if erpSO_missing == "✗" or erpPO_missing == "✗":
            results.append({
                "Chassis": row["Chassis"],
                "Chassis_Clean": chassis,
                "Regent Production": row["Regent Production"],
                "Dealer": row["Dealer"],
                "Model": row["Model"],
                "Customer": row["Customer"],
                "erpSO": erpSO,
                "erpPO": erpPO,
                "erpSO_norm": erpSO_norm,
                "erpPO_norm": erpPO_norm,
                "erpSO_missing": erpSO_missing,
                "erpPO_missing": erpPO_missing,
                "error_type": "; ".join([
                    x for x in [
                        "erpSO 缺失" if erpSO_missing == "✗" else None,
                        "erpPO 缺失" if erpPO_missing == "✗" else None
                    ] if x
                ])
            })

    orders_po_missing = pd.DataFrame(results)

    # ===========================
    # 导出
    # ===========================
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"API_Error_Report_{timestamp}.xlsx"

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        instore_df.to_excel(writer, sheet_name="instore_raw", index=False)
        main_df.to_excel(writer, sheet_name="orders_raw", index=False)
        ol_dedup.to_excel(writer, sheet_name="orderlist_clean", index=False)
        bp_dedup.to_excel(writer, sheet_name="bp_clean", index=False)

        instore_error_final.to_excel(writer, sheet_name="instore_errors", index=False)
        final_error_report.to_excel(writer, sheet_name="orders_errors", index=False)
        orders_po_missing.to_excel(writer, sheet_name="orders_po_missing", index=False)

        summary = pd.DataFrame([
            {"item": "inStore 原始条数", "value": len(instore_df)},
            {"item": "inStore 缺失 erpSO 条数", "value": len(instore_error_final)},
            {"item": "orders 原始条数", "value": len(main_df)},
            {"item": "orders 匹配到 Orderlist 条数", "value": len(merged)},
            {"item": "orders erpPONumber 缺失条数", "value": len(orders_po_missing)},
            {"item": "orders 校验异常总条数", "value": len(final_error_report)},
        ])
        summary.to_excel(writer, sheet_name="summary", index=False)

    print("🎉 已生成文件:", filename)


if __name__ == "__main__":
    build_report()
