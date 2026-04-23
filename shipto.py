# -*- coding: utf-8 -*-
# zvkbur_shipto_check_export.py

import io
import re
import logging
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pyodbc

# ========= Logging =========
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("zvkbur_shipto_check")

# ========= SAP HANA =========
DSN = (
    "DRIVER={HDBODBC};"
    "SERVERNODE=10.11.2.25:30241;"
    "UID=BAOJIANFENG;"
    "PWD=Xja@2025ABC;"
)

SAP_CLIENT = "800"

# ========= SharePoint 直链 =========
ORDERLIST_PUBLIC_DL = "https://regentrv-my.sharepoint.com/:x:/g/personal/planning_regentrv_com_au/ETevaCJOE_ZLqQt1ZH4mcUkBm_zrJBIN5TrKkx6tRn-7_w?e=cff2ie&download=1"

# ========= 核对清单 =========
VALID_LOCATIONS = {"3141", "3121", "3123", "3126", "3128"}

# ========= SQL =========
MAIN_SQL = rf"""
WITH ship_to AS (
    SELECT 
        "MANDT",
        "VBELN",
        MAX(CASE WHEN "PARVW" = 'WE' THEN "KUNNR" END) AS "SHIP_TO"
    FROM "SAPHANADB"."VBPA"
    WHERE "MANDT" = '{SAP_CLIENT}'
    GROUP BY "MANDT", "VBELN"
)

SELECT 
    vbap."VBELN" AS "Sales Order",
    vbap."POSNR" AS "Item No",
    vbap."MATNR" AS "Material No",
    objk."SERNR" AS "Chassis No",
    st."SHIP_TO" AS "Ship-to Code",
    vbak."ZVKBUR" AS "Actual Location SAP",
    vbak."VKORG" AS "Sales Company"

FROM "SAPHANADB"."VBAP" vbap

LEFT JOIN "SAPHANADB"."VBAK" vbak
    ON vbap."MANDT" = vbak."MANDT"
   AND vbap."VBELN" = vbak."VBELN"

LEFT JOIN ship_to st
    ON vbap."MANDT" = st."MANDT"
   AND vbap."VBELN" = st."VBELN"

LEFT JOIN "SAPHANADB"."SER02" s
    ON vbap."MANDT" = s."MANDT"
   AND vbap."VBELN" = s."SDAUFNR"
   AND s."POSNR" = 10

LEFT JOIN "SAPHANADB"."OBJK" objk
    ON s."MANDT" = objk."MANDT"
   AND s."OBKNR" = objk."OBKNR"

WHERE
    vbap."MANDT" = '{SAP_CLIENT}'
    AND vbap."POSNR" = 10
"""

# ========= HTTP =========
def http_get_with_retry(url: str, timeout: int = 60) -> bytes:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    headers = {"User-Agent": "Mozilla/5.0"}

    u = url.replace(" ", "%20")
    candidates = [u] + ([f"{u}{'&' if '?' in u else '?'}download=1"] if "download=1" not in u else [])

    last_err: Optional[Exception] = None
    for cand in candidates:
        try:
            resp = sess.get(cand, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            log.warning("Download failed: %s -> %s", cand, e)

    raise last_err if last_err else RuntimeError("Download failed")

def looks_like_excel_zip(b: bytes) -> bool:
    return len(b) > 4 and b[:2] == b"PK" and (b.find(b"xl/") != -1)

def fetch_excel_bytes(public_dl: str) -> bytes:
    content = http_get_with_retry(public_dl)
    if not looks_like_excel_zip(content):
        raise ValueError("Downloaded content is not an Excel file.")
    return content

# ========= HANA =========
def hana_query(sql: str) -> pd.DataFrame:
    with pyodbc.connect(DSN, autocommit=True) as conn:
        return pd.read_sql(sql, conn)

# ========= Helpers =========
def clean_chassis(x):
    if pd.isna(x):
        return None
    s = str(x).strip().replace("-", "")
    s = re.sub(r"[^A-Za-z0-9]", "", s)
    return s or None

def to_str_no_leading_zeros(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    s2 = s.lstrip("0")
    return s2 if s2 != "" else "0"

def clean_text(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    return s if s != "" else None

def read_excel_bytes_select_sheet(xls_bytes: bytes, preferred_name: str) -> pd.DataFrame:
    xfile = pd.ExcelFile(io.BytesIO(xls_bytes), engine="openpyxl")
    sheets = xfile.sheet_names

    if preferred_name in sheets:
        return pd.read_excel(xfile, sheet_name=preferred_name)

    def norm(s: str) -> str:
        return re.sub(r"\s+", "", s or "", flags=re.UNICODE).lower()

    target = norm(preferred_name)

    for s in sheets:
        if norm(s) == target:
            return pd.read_excel(xfile, sheet_name=s)

    for s in sheets:
        if target in norm(s):
            return pd.read_excel(xfile, sheet_name=s)

    return pd.read_excel(xfile, sheet_name=sheets[0])

# ========= 主逻辑 =========
def build_final_dataframe(orderlist_bytes: bytes) -> pd.DataFrame:
    log.info("Querying SAP HANA ...")
    main = hana_query(MAIN_SQL)

    # 清洗 SAP 数据
    main["Chassis_Clean"] = main["Chassis No"].apply(clean_chassis)
    main["Ship-to Code"] = main["Ship-to Code"].apply(to_str_no_leading_zeros)
    main["Actual Location SAP"] = main["Actual Location SAP"].apply(to_str_no_leading_zeros)

    # 读取 Orderlist
    log.info("Reading Orderlist ...")
    ol = read_excel_bytes_select_sheet(orderlist_bytes, "Orderlist")

    if "Chassis" not in ol.columns:
        raise ValueError("Orderlist sheet does not contain column: Chassis")

    ol["Chassis_Clean"] = ol["Chassis"].apply(clean_chassis)

    # 如果有这些列就带上，没有就补空
    for c in ["Regent Production", "Model", "Dealer", "Customer"]:
        if c not in ol.columns:
            ol[c] = None

    ol_dedup = (
        ol[["Chassis", "Chassis_Clean", "Regent Production", "Model", "Dealer", "Customer"]]
        .dropna(subset=["Chassis_Clean"])
        .drop_duplicates(subset=["Chassis_Clean"], keep="first")
    )

    valid_chassis = set(ol_dedup["Chassis_Clean"].dropna().unique())

    # 只保留 Orderlist 里的 chassis
    merged = main[main["Chassis_Clean"].isin(valid_chassis)].copy()

    # 回带 Orderlist 信息
    merged = merged.merge(
        ol_dedup[["Chassis_Clean", "Regent Production", "Model", "Dealer", "Customer"]],
        how="left",
        on="Chassis_Clean"
    )

    # 过滤 1：Regent Production = finished 的不显示
    merged = merged[
        ~merged["Regent Production"]
        .astype(str)
        .str.strip()
        .str.lower()
        .eq("finished")
    ].copy()

    # 过滤 2：只保留销售公司 3110
    merged = merged[
        merged["Sales Company"]
        .astype(str)
        .str.strip()
        .eq("3110")
    ].copy()

    def location_check_row(r):
        zvkbur = clean_text(r.get("Actual Location SAP"))
        shipto = clean_text(r.get("Ship-to Code"))

        if zvkbur in VALID_LOCATIONS or shipto in VALID_LOCATIONS:
            return "Match"
        return "Not Match"

    def location_hit_detail(r):
        zvkbur = clean_text(r.get("Actual Location SAP"))
        shipto = clean_text(r.get("Ship-to Code"))

        z_hit = zvkbur in VALID_LOCATIONS
        s_hit = shipto in VALID_LOCATIONS

        if z_hit and s_hit:
            return "Both"
        if z_hit:
            return "ZVKBUR Only"
        if s_hit:
            return "Ship-to Only"
        return "None"

    merged["Check Result"] = merged.apply(location_check_row, axis=1)
    merged["Hit Detail"] = merged.apply(location_hit_detail, axis=1)
    merged["ZVKBUR_in_List"] = merged["Actual Location SAP"].isin(VALID_LOCATIONS)
    merged["ShipTo_in_List"] = merged["Ship-to Code"].isin(VALID_LOCATIONS)

    # 过滤 3：Actual Location SAP 或 Ship-to Code 至少一个命中 VALID_LOCATIONS
    merged = merged[
        merged["ZVKBUR_in_List"] | merged["ShipTo_in_List"]
    ].copy()

    final = merged[[
        "Sales Order",
        "Item No",
        "Material No",
        "Chassis No",
        "Ship-to Code",
        "Actual Location SAP",
        "Check Result",
        "Hit Detail",
        "ZVKBUR_in_List",
        "ShipTo_in_List",
        "Regent Production",
        "Model",
        "Dealer",
        "Customer",
    ]].copy()

    final = final.drop_duplicates(subset=["Chassis No"], keep="first").reset_index(drop=True)
    return final

# ========= main =========
if __name__ == "__main__":
    log.info("Downloading Orderlist ...")
    orderlist_bytes = fetch_excel_bytes(ORDERLIST_PUBLIC_DL)

    df = build_final_dataframe(orderlist_bytes)

    output_file = "ZVKBUR_ShipTo_Check.xlsx"
    df.to_excel(output_file, index=False)

    log.info("Done. Output file: %s", output_file)
