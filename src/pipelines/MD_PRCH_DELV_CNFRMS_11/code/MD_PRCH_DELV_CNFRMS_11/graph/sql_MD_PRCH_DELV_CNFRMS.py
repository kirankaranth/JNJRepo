from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_PRCH_DELV_CNFRMS_11.config.ConfigStore import *
from MD_PRCH_DELV_CNFRMS_11.udfs.UDFs import *

def sql_MD_PRCH_DELV_CNFRMS(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
      SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD
, TRIM(f4311_adt.pddoco) as PO_NUM
, TRIM(f4311_adt.pdlnid) as PO_LINE_NBR
,-1 AS CNFRM_SEQ_NBR
, TRIM(f4311_adt.pddcto) as CNFRM_CAT_CD
, CASE WHEN LOWER(TRIM(f4311_adt.pdpddj)) = 'CAST(NULL AS timestamp)' OR TRIM(f4311_adt.pdpddj) = '' OR TRIM(f4311_adt.pdpddj) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f4311_adt.pdpddj) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f4311_adt.pdpddj) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS DELV_DTTM
, CASE WHEN LOWER(TRIM(f4311_adt.pdtrdj)) = 'CAST(NULL AS timestamp)' OR TRIM(f4311_adt.pdtrdj) = '' OR TRIM(f4311_adt.pdtrdj) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f4311_adt.pdtrdj) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f4311_adt.pdtrdj) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_ON_DTTM
, CAST(TRIM(f4311_adt.pduorg) AS decimal(18,4)) as CNFRM_QTY
, NULL as MRP_ADJ_QTY
, TRIM(f4311_adt.pdrorn) as SLS_ORDR_NUM
, TRIM(f4311_adt.pdrlln) as SLS_ORDR_LINE_NBR
, TRIM(f4311_adt.pdlotn) as VEND_BTCH_NUM
, NULL as REF_DOC_NUM
, TRIM(f4311_adt.pdkcoo) as CO_CD
, TRIM(f4311_adt.pdsfxo) as ORDER_SUF
, NULL as DT_CAT_OF_DELV_DT_SUP_CNFRM
, NULL as CRT_IN_SUP_CNFRM
, NULL as SUP_CNFRM_DEL_IN
, NULL as CNFRM_RLVNT_TO_MATL_PLNG
, NULL as MFR_PART_PRFL
, NULL as MATL_NUM_CRSPN_TO_MFR_PART_NUM
, NULL as NUM_OF_RMNDR
, NULL as HI_LVL_ITM_OF_BTCH_SPLT_ITM
, NULL as SEQ_NUM_OF_SUP_CNFRM
, NULL as DELV_HAS_STS_IN_PLNT
, NULL as DELV
, NULL as DELV_ITM
, NULL as HANDOVR_DTTM
, NULL as STK_SGMNT
, NULL as UTC_TMST
, NULL as QTY_OF_ORDR_CNFRM
, NULL as MRP_REDUC_QTY
, NULL as PER_OF_PERF_STRT_DTTM
, NULL as PER_OF_PERF_END_DTTM
, NULL as SRVC_PERFMR
, NULL as EXPTD_VAL_OF_OVRL_LMT
, NULL as SUP_CNFRM_NUM
, NULL as SUP_CNFRM_ITM
, NULL as ALC_STK_QTY
, NULL as ORIG_QTY_OF_SHIPPING_NTF
, NULL as REF_UUID_OF_TRSPN_MGMT
, f4311_adt._upt_ as _l0_upt_
 FROM
  {Config.sourceDatabase}.f4311_adt f4311_adt
WHERE
  f4311_adt._deleted_ = 'F'


  
 
"""
    )

    return out0
