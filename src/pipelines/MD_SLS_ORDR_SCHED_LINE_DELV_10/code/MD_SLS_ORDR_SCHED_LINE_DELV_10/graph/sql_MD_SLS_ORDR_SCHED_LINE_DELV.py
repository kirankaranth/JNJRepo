from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_SLS_ORDR_SCHED_LINE_DELV_10.config.ConfigStore import *
from MD_SLS_ORDR_SCHED_LINE_DELV_10.udfs.UDFs import *

def sql_MD_SLS_ORDR_SCHED_LINE_DELV(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD,
  VBEP.POSNR AS SCHED_LINE_ITM_NUM,
  VBEP.VBELN AS SLS_ORDR_DOC_ID,
  VBEP.ETENR AS SCHED_LINE_NUM,
  VBAK.BUKRS_VF AS CO_CD,
  VBAK.AUART AS SLS_ORDR_TYPE_CD,
  CASE
    WHEN VBEP.MBDAT = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(CONCAT(VBEP.MBDAT, VBEP.MBUHR), 'yyyyMMddHHmmss')
  END AS MATL_AVLBLTY_DTTM,
  CAST(TRIM(VBEP.BMENG) AS DECIMAL(18, 4)) AS CNFRM_QTY,
  CASE
    WHEN VBEP.EDATU = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(VBEP.EDATU, \"yyyyMMdd\")
  END AS RQST_DELV_DTTM,
  CASE
    WHEN VBEP.WADAT = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(CONCAT(VBEP.WADAT, VBEP.WAUHR), 'yyyyMMddHHmmss')
  END AS GI_DTTM,
  CAST(TRIM(VBEP.WMENG) AS DECIMAL(18, 4)) AS SLS_UNIT_ORDR_QTY,
  TRIM(VBEP.VRKME) AS SLS_UOM_CD,
  TRIM(VBEP.LIFSP) AS DELV_BLK_CD,
  TRIM(TVLST.VTEXT) AS DELV_BLK_DESC,
  CASE
    WHEN VBEP.TDDAT = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(CONCAT(VBEP.TDDAT, VBEP.TDUHR), 'yyyyMMddHHmmss')
  END AS TRSPN_PLAN_DTTM,
  CASE
    WHEN VBEP.LDDAT = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(CONCAT(VBEP.LDDAT, VBEP.LDUHR), 'yyyyMMddHHmmss')
  END AS LD_DTTM,
VBEP._upt_ as _l0_upt_
FROM
  {Config.sourceDatabase}.VBEP VBEP
  LEFT JOIN {Config.sourceDatabase}.VBAK VBAK ON VBEP.VBELN = VBAK.VBELN
  AND VBAK._deleted_ = 'F'
  AND VBAK.MANDT = '{Config.MANDT}'
  LEFT JOIN {Config.sourceDatabase}.TVLST TVLST ON VBEP.LIFSP = TVLST.LIFSP
  AND TVLST._deleted_ = 'F'
  AND TVLST.SPRAS = 'E'
  AND TVLST.MANDT = '{Config.MANDT}'
WHERE
  VBEP._deleted_ = 'F'
  AND VBEP.MANDT = '{Config.MANDT}'  
 
"""
    )

    return out0
