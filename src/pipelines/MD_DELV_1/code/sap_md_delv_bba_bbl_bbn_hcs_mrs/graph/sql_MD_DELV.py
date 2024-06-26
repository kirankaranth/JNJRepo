from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_bba_bbl_bbn_hcs_mrs.config.ConfigStore import *
from sap_md_delv_bba_bbl_bbn_hcs_mrs.udfs.UDFs import *

def sql_MD_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT 
'{Config.sourceSystem}' AS SRC_SYS_CD,
SUB_VBAK.BUKRS_VF AS CO_CD,
LIKP.VBELN AS DELV_NUM,
LIKP.LFART AS DELV_TYP_CD,
TRIM(TVLKT.VTEXT) AS DELV_TYP_DESC,
TRIM(LIKP.BOLNR) AS BILL_OF_LDNG_NUM,
--SUB_VBUK.UVFAS AS BILL_ICMPT_TOT_STS_CD,
TRIM(VBUK.UVFAS) AS BILL_ICMPT_TOT_STS_CD,
TRIM(VBUK.FKSTK) AS BILL_STS_CD,
case when LIKP.AEDAT= '00000000' then null else to_timestamp(LIKP.AEDAT, \"yyyyMMdd\") end as CHG_DTTM,
TRIM(VBUK.BESTK) AS CNFRM_STS_CD,
CASE WHEN LIKP.ERDAT = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.ERDAT, LIKP.ERZET),'yyyyMMddHHmmss') end as CRT_DTTM,
TRIM(VBUK.CMGST) AS CR_CHK_TOT_STS_CD,
CASE WHEN LIKP.LFDAT = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.LFDAT, LIKP.LFUHR),'yyyyMMddHHmmss') end as DELV_DTTM,
TRIM(LIKP.SPE_SHP_INF_STS) AS SHIP_STS_CD,
TRIM(LIKP.SDABW) AS SPL_PRCS_IN,
TRIM(LIKP.VKORG) AS SLS_ORG_NUM,
TRIM(TVKOT.VTEXT) AS SLS_ORG_NM,
TRIM(VBUK.UVVLS) AS DELV_ICMPT_TOT_STS_CD,
TRIM(VBUK.LFSTK) AS DELV_STS_CD,
TRIM(VBUK.LFGSK) AS DELV_TOT_STS_CD,
TRIM(LIKP.ROUTE) AS RTE_ID,
TRIM(LIKP.LIFEX) AS DELIVERY_NUM,
CAST(NULL AS string) as REF_DOC_NUM,
TRIM(VBUK.UVWAS) AS GM_ICMPT_TOT_STS_CD,
TRIM(VBUK.WBSTK) AS GM_TOT_STS_CD,
TRIM(LIKP.IMWRK) AS IN_PLNT_IND,
TRIM(VBUK.UVALS) AS ICMPT_TOT_STS_CD,
TRIM(VBUK.FKIVK) AS INTCO_BILL_TOT_STS_CD,
TRIM(VBUK.RELIK) AS INVC_LIST_STS_CD,
TRIM(VBUK.FKSAK) AS ORDR_BILL_STS_CD,
TRIM(VBUK.UVPAK) AS PACK_ICMPT_STS_CD,
TRIM(VBUK.UVPAS) AS PACK_ICMPT_TOT_STS_CD,
TRIM(VBUK.PKSTK) AS PACK_TOT_STS_CD,
TRIM(VBUK.KOQUK) AS PICK_CNFRM_STS_CD,
TRIM(VBUK.UVPIK) AS PICK_ICMPT_STS_CD,
TRIM(VBUK.UVPIS) AS PICK_ICMPT_TOT_STS_CD,
TRIM(VBUK.KOSTK) AS PICK_TOT_STS_CD,
CASE WHEN LIKP.WADAT = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.WADAT, LIKP.WAUHR),'yyyyMMddHHmmss') end as PLAN_GI_DTTM,
case when LIKP.WADAT_IST= '00000000' then null else to_timestamp(LIKP.WADAT_IST, \"yyyyMMdd\") end as ACTL_GI_DTTM,
TRIM(LIKP.WERKS) AS PLNT_CD,
TRIM(VBUK.BUCHK) AS PSTNG_BILL_STS_CD,
TRIM(VBUK.UVPRS) AS PRC_ICMPT_STS_CD,
TRIM(LIKP.TERNR) AS PRCS_ORDR_NUM,
TRIM(VBUK.GBSTK) AS PRCSG_TOT_STS_CD,
TRIM(VBUK.RFSTK) AS REF_DOC_STS_CD,
TRIM(VBUK.RFGSK) AS REF_DOC_TOT_STS_CD,
TRIM(VBUK.ABSTK) AS REJ_TOT_STS_CD,
TRIM(LIKP.VBTYP) AS SLS_ORDR_CAR_CD,
TRIM(LIKP.KUNNR) AS SHIP_TO_CUST_NUM,
TRIM(LIKP.VSBED) AS SHIPPING_COND_CD,
TRIM(LIKP.VSTEL) AS SHIPPING_PT_NUM,
TRIM(LIKP.KUNAG) AS SOLD_TO_CUST_NUM,
TRIM(LIKP.LIFNR) AS SUP_NUM,
CAST(TRIM(LIKP.ANZPK) AS INT) AS TOT_PKGS_CNT,
TRIM(VBUK.TRSTA) AS TRNSP_PLAN_STS_CD,
TRIM(VBUK.LVSTK) AS WM_TOT_STS_CD,
CASE WHEN LIKP.KODAT = '00000000' THEN NULL ELSE to_timestamp(concat(LIKP.KODAT, LIKP.KOUHR),'yyyyMMddHHmmss') end as PICK_DTTM,
TRIM(LIKP.WAERK) AS SLS_ORDR_CRNCY_CD,
TRIM(VBUK.CMPS1) AS RESV_CD,
TRIM(VBUK.UVALL) AS OVRL_HDR_CD,
TRIM(VBUK.FSSTK) AS BILL_BLK_STS_CD,
TRIM(VBUK.LSSTK) AS DELV_BLK_STS_CD,
TRIM(VBUK.SPSTG) AS OVRL_BLK_STS_CD,
case when LIKP.PODAT= '00000000' then null else to_timestamp(LIKP.PODAT, \"yyyyMMdd\") end as POD_DTTM,
CAST(TRIM(LIKP.BTGEW) AS DECIMAL(18,4)) AS TOT_WT_CD,
TRIM(LIKP.XABLN) AS GR_SLIP_NUM,
TRIM(LIKP.TRAID) AS MEANS_TRNSP_ID,
'#' AS SRC_TBL_NM,
TRIM(LIKP.XBLNR) as SHIP_REF_NUM,
CAST(NULL AS string) as MODE_TRSPN_BORDR,
TRIM(TVLS.LIFSP) as DFLT_DELV_BLK_CD,
TRIM(TVLST.VTEXT) as DFLT_DELV_BLK_DESC,
TRIM(TVLS.SPEBE) as CNFRM_BLK_CD,
TRIM(TVLS.SPELF) as DELV_BLK_CD,
TRIM(TVSTT.VTEXT) as SHIPPING_PT_DESC,
TRIM(LIKP.TRSPG) as SHIP_BLOK_RSN_CD,
CAST(NULL AS string) as SHIP_BLOK_RSN_DESC,
TRIM(TVRO.SPFBK) as FCTRY_CAL_KEY,
TRIM(TVROT.BEZEI) as RTE_DESC,
CAST(TRIM(TVRO.TDVZTD) AS decimal (18,4)) as TRSPN_LEAD_TIME_IN_CAL_DAYS,
CAST(TRIM(TVRO.TRAZTD) AS decimal (18,4)) as TRST_DUR_IN_CAL_DAYS,
LIKP._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.LIKP LIKP
LEFT JOIN (select distinct (VBAK.BUKRS_VF) as BUKRS_VF , LIPS.VBELN AS VBELN from {Config.sourceDatabase}.LIPS LEFT JOIN {Config.sourceDatabase}.VBAK
ON LIPS.VGBEL=VBAK.VBELN and VBAK._deleted_ = 'F' AND VBAK.MANDT = \"100\" where
LIPS._deleted_ = 'F' AND LIPS.MANDT = \"100\" and TRIM(LIPS.VGBEL) <> '')SUB_VBAK ON LIKP.VBELN=SUB_VBAK.VBELN
LEFT JOIN {Config.sourceDatabase}.TVLKT TVLKT ON LIKP.LFART=TVLKT.LFART AND TVLKT.SPRAS = 'E' AND TVLKT._deleted_ = 'F' AND TVLKT.MANDT = \"100\"
--LEFT JOIN (select distinct TRIM(VBUK.UVFAS) as UVFAS , LIPS.VBELN AS VBELN from {Config.sourceDatabase}.LIPS LEFT JOIN {Config.sourceDatabase}.VBUK
--ON LIPS.VGPOS=VBUK.VBELN and VBUK._deleted_ = 'F' AND VBUK.MANDT = \"100\" where LIPS._deleted_ = 'F' AND LIPS.MANDT = \"100\"
--) SUB_VBUK
--ON LIKP.VBELN=SUB_VBUK.VBELN
LEFT JOIN {Config.sourceDatabase}.VBUK VBUK ON LIKP.VBELN=VBUK.VBELN AND VBUK._deleted_ = 'F'  AND VBUK.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVKOT TVKOT ON LIKP.VKORG=TVKOT.VKORG AND TVKOT.SPRAS = 'E'  AND TVKOT._deleted_ = 'F' AND TVKOT.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVLS AS TVLS ON LIKP.LIFSK=TVLS.LIFSP AND  TVLS._deleted_ = 'F' AND TVLS.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVLST AS TVLST ON LIKP.LIFSK=TVLST.LIFSP AND TVLST.SPRAS = 'E' AND TVLST._deleted_ = 'F' AND TVLST.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVSTT AS TVSTT ON LIKP.VSTEL=TVSTT.VSTEL  AND TVSTT.SPRAS = 'E' AND TVSTT._deleted_ = 'F' AND TVSTT.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVRO AS TVRO ON LIKP.ROUTE=TVRO.ROUTE AND TVRO._deleted_ = 'F' AND TVRO.MANDT = \"100\"
LEFT JOIN {Config.sourceDatabase}.TVROT AS TVROT ON LIKP.ROUTE=TVROT.ROUTE AND TVROT.SPRAS = 'E' AND TVROT._deleted_ = 'F' AND TVROT.MANDT = \"100\"
WHERE LIKP._deleted_ = 'F' AND LIKP.MANDT = \"100\"
  
 
"""
    )

    return out0
