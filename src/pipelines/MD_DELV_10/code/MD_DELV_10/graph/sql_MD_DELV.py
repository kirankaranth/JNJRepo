from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_DELV_10.config.ConfigStore import *
from MD_DELV_10.udfs.UDFs import *

def sql_MD_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT DISTINCT
'{Config.sourceSystem}' AS SRC_SYS_CD,
VBAK.BUKRS_VF AS CO_CD,
LIKP.VBELN as DELV_NUM,
LIKP.LFART as DELV_TYP_CD,
TRIM(TVLKT.VTEXT) AS DELV_TYP_DESC,
TRIM(LIKP.BOLNR) AS BILL_OF_LDNG_NUM,
NULL AS BILL_ICMPT_TOT_STS_CD,
NULL AS BILL_STS_CD,
CASE WHEN LIKP.AEDAT = '00000000' then Null else to_timestamp(LIKP.AEDAT,'yyyyMMdd') END AS CHG_DTTM,
NULL AS CNFRM_STS_CD,
CASE WHEN LIKP.ERDAT = '00000000' OR LIKP.ERZET = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.ERDAT, LIKP.ERZET),'yyyyMMddHHmmss') end as CRT_DTTM,
NULL AS CR_CHK_TOT_STS_CD,
CASE WHEN LIKP.LFDAT = '00000000' OR LIKP.LFUHR = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.LFDAT, LIKP.LFUHR),'yyyyMMddHHmmss') end as DELV_DTTM,
TRIM(LIKP.SPE_SHP_INF_STS) AS SHIP_STS_CD,
TRIM(LIKP.SDABW) AS SPL_PRCS_IN,
TRIM(LIKP.VKORG) AS SLS_ORG_NUM,
TRIM(TVKOT.VTEXT) AS SLS_ORG_NM,
NULL AS DELV_ICMPT_TOT_STS_CD,
TRIM(VBAK.LFSTK) AS DELV_STS_CD,
TRIM(VBAK.LFGSK) AS DELV_TOT_STS_CD,
TRIM(LIKP.ROUTE) AS RTE_ID,
TRIM(LIKP.LIFEX) AS DELIVERY_NUM,
LIKP.XBLNR AS REF_DOC_NUM,
NULL AS GM_ICMPT_TOT_STS_CD,
NULL AS GM_TOT_STS_CD,
TRIM(LIKP.IMWRK) AS IN_PLNT_IND,
NULL AS ICMPT_TOT_STS_CD,
NULL AS INTCO_BILL_TOT_STS_CD,
TRIM(VBRK.RELIK) AS INVC_LIST_STS_CD,
TRIM(VBAK.FKSAK) AS ORDR_BILL_STS_CD,
NULL AS PACK_ICMPT_STS_CD,
NULL AS PACK_ICMPT_TOT_STS_CD,
NULL AS PACK_TOT_STS_CD,
NULL AS PICK_CNFRM_STS_CD,
NULL AS PICK_ICMPT_STS_CD,
NULL AS PICK_ICMPT_TOT_STS_CD,
NULL AS PICK_TOT_STS_CD,
CASE WHEN LIKP.WADAT = '00000000' OR LIKP.WAUHR = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.WADAT, LIKP.WAUHR),'yyyyMMddHHmmss') end as PLAN_GI_DTTM,
case when LIKP.WADAT_IST= '00000000' then null else to_timestamp(LIKP.WADAT_IST, \"yyyyMMdd\") end as ACTL_GI_DTTM,
TRIM(LIKP.WERKS) AS PLNT_CD,
TRIM(VBRK.BUCHK) AS PSTNG_BILL_STS_CD,
TRIM(VBAK.UVPRS) AS PRC_ICMPT_STS_CD,
TRIM(LIKP.TERNR) AS PRCS_ORDR_NUM,
NULL AS PRCSG_TOT_STS_CD,
TRIM(VBAK.RFSTK) AS REF_DOC_STS_CD,
TRIM(VBAK.RFGSK) AS REF_DOC_TOT_STS_CD,
TRIM(VBAK.ABSTK) AS REJ_TOT_STS_CD,
TRIM(LIKP.VBTYP) AS SLS_ORDR_CAR_CD,
TRIM(LIKP.KUNNR) AS SHIP_TO_CUST_NUM,
TRIM(LIKP.VSBED) AS SHIPPING_COND_CD,
TRIM(LIKP.VSTEL) AS SHIPPING_PT_NUM,
TRIM(LIKP.KUNAG) AS SOLD_TO_CUST_NUM,
TRIM(LIKP.LIFNR) AS SUP_NUM,
CAST(TRIM (LIKP.ANZPK) AS INT ) AS TOT_PKGS_CNT,
NULL AS TRNSP_PLAN_STS_CD,
NULL AS WM_TOT_STS_CD,
CASE WHEN LIKP.KODAT = '00000000' OR LIKP.KOUHR = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.KODAT, LIKP.KOUHR),'yyyyMMddHHmmss') end as PICK_DTTM,
TRIM(LIKP.WAERK) AS SLS_ORDR_CRNCY_CD,
NULL AS RESV_CD,
TRIM(LIKP.UVALL) AS OVRL_HDR_CD,
TRIM(VBAK.FSSTK) AS BILL_BLK_STS_CD,
TRIM(VBAK.LSSTK) AS DELV_BLK_STS_CD,
TRIM(LIKP.SPSTG) AS OVRL_BLK_STS_CD,
case when LIKP.PODAT= '00000000' then null else to_timestamp(LIKP.PODAT, 'yyyyMMdd') end as POD_DTTM,
CAST(TRIM(LIKP.BTGEW) AS DECIMAL(18,4)) AS TOT_WT_CD,
TRIM(LIKP.XABLN) AS GR_SLIP_NUM,
TRIM(LIKP.TRAID) AS MEANS_TRNSP_ID,
'#' AS SRC_TBL_NM,
LIKP._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.LIKP LIKP
LEFT JOIN {Config.sourceDatabase}.LIPS LIPS on LIKP.VBELN=LIPS.VBELN
AND LIPS._deleted_ = 'F' AND LIPS.MANDT = '{Config.MANDT}' and TRIM(LIPS.VGBEL) <> ''
LEFT JOIN {Config.sourceDatabase}.VBAK VBAK on LIPS.VGBEL=VBAK.VBELN
AND VBAK._deleted_ = 'F' AND VBAK.MANDT = '{Config.MANDT}'
LEFT JOIN {Config.sourceDatabase}.TVLKT  TVLKT on LIKP.LFART = TVLKT.LFART
AND TVLKT.SPRAS = \"E\" AND  TVLKT._deleted_ = 'F' AND TVLKT.MANDT = '{Config.MANDT}'
LEFT JOIN {Config.sourceDatabase}.TVKOT on LIKP.VKORG = TVKOT.VKORG
AND TVKOT.SPRAS = \"E\" AND  TVKOT._deleted_ = 'F' AND TVKOT.MANDT = '{Config.MANDT}'
LEFT JOIN {Config.sourceDatabase}.VBRK VBRK on LIKP.VBELN=VBRK.VBELN
AND VBRK._deleted_ = 'F' AND VBRK.MANDT = '{Config.MANDT}'
WHERE LIKP._deleted_ = 'F' AND LIKP.MANDT = '{Config.MANDT}'  
 
"""
    )

    return out0
