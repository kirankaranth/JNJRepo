from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_prch_delv_cnfrms_10_bwi.config.ConfigStore import *
from md_prch_delv_cnfrms_10_bwi.udfs.UDFs import *

def sql_MD_PRCH_DELV_CNFRMS(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(ekes.ebeln) as PO_NUM
, TRIM(ekes.ebelp) as PO_LINE_NBR
, CAST(TRIM(ekes.etens) AS INT) as CNFRM_SEQ_NBR
, TRIM(ekes.ebtyp) as CNFRM_CAT_CD
, case when Trim(ekes.eindt) = '00000000' then CAST(NULL AS timestamp)
else to_timestamp(Trim(ekes.eindt),\"yyyyMMdd\") end as DELV_DTTM
, case when Trim(ekes.erdat) = '00000000' then CAST(NULL AS timestamp)
else to_timestamp(Trim(ekes.erdat),\"yyyyMMdd\") end as CRT_ON_DTTM
, CAST(TRIM(ekes.menge) AS decimal(18,4)) as CNFRM_QTY
, CAST(TRIM(ekes.dabmg) AS decimal(18,4)) as MRP_ADJ_QTY
, TRIM(ekes.vbeln) as SLS_ORDR_NUM
, TRIM(ekes.vbelp) as SLS_ORDR_LINE_NBR
, TRIM(ekes.charg) as VEND_BTCH_NUM
, NULL as REF_DOC_NUM
, '#' as CO_CD
, '#' as ORDER_SUF
, TRIM(ekes.lpein) as DT_CAT_OF_DELV_DT_SUP_CNFRM
, TRIM(ekes.estkz) as CRT_IN_SUP_CNFRM
, TRIM(ekes.loekz) as SUP_CNFRM_DEL_IN
, TRIM(ekes.kzdis) as CNFRM_RLVNT_TO_MATL_PLNG
, TRIM(ekes.mprof) as MFR_PART_PRFL
, TRIM(ekes.ematn) as MATL_NUM_CRSPN_TO_MFR_PART_NUM
, CAST(TRIM(ekes.mahnz) AS decimal(18,4)) as NUM_OF_RMNDR
, TRIM(ekes.uecha) as HI_LVL_ITM_OF_BTCH_SPLT_ITM
, TRIM(ekes.ref_etens) as SEQ_NUM_OF_SUP_CNFRM
, TRIM(ekes.imwrk) as DELV_HAS_STS_IN_PLNT
, TRIM(ekes.vbeln_st) as DELV
, TRIM(ekes.vbelp_st) as DELV_ITM
,CASE WHEN ekes.handoverdate = '00000000' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(concat(ekes.handoverdate, ekes.handovertime),\"yyyyMMddHHmmss\") END AS HANDOVR_DTTM
, trim(sgt_scat) AS STK_SGMNT
, NULL as UTC_TMST
, NULL as QTY_OF_ORDR_CNFRM
, NULL as MRP_REDUC_QTY
, NULL as PER_OF_PERF_STRT_DTTM
, NULL as PER_OF_PERF_END_DTTM
, NULL as SRVC_PERFMR
, NULL as EXPTD_VAL_OF_OVRL_LMT
, NULL as SUP_CNFRM_NUM
, NULL as SUP_CNFRM_ITM
, CAST(TRIM(ekes.fsh_salloc_qty) AS decimal(18,4)) as ALC_STK_QTY
, NULL as ORIG_QTY_OF_SHIPPING_NTF
, NULL as REF_UUID_OF_TRSPN_MGMT,
ekes._upt_ as _l0_upt_
FROM
  {Config.sourceDatabase}.ekes ekes
WHERE
  ekes._deleted_ = 'F'
  AND ekes.mandt = '400'
  
 
"""
    )

    return out0
