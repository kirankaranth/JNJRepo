from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_prch_info_hmd.config.ConfigStore import *
from sap_md_prch_info_hmd.udfs.UDFs import *

def sql_MD_PRCH_INFO(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
  EINA.infnr AS PRCH_INFO_NUM,
  TRIM(EINA.matnr) AS MATL_NUM,
  TRIM(EINA.matkl) AS MATL_GRP_CD,
  TRIM(EINA.lifnr) AS SUP_NUM,
  TRIM(EINA.loekz) AS DEL_IND,
  CASE
    WHEN EINA.erdat = '00000000' THEN CAST(NULL AS TIMESTAMP)
    ELSE TO_TIMESTAMP(EINA.erdat, 'yyyyMMdd')
  END AS CRT_ON_DTTM,
  TRIM(EINA.ernam) AS CRT_BY_NM,
  TRIM(EINA.txz01) AS PRCH_INFO_DESC,
  TRIM(EINA.meins) AS PO_UOM_CD,
  TRIM(EINA.idnlf) AS MFR_PART_NUM,
  TRIM(EINA.mfrnr) AS MFR_NUM,
  EINA._upt_ as _l0_upt_,
  EINA._deleted_
FROM
  {Config.sourceDatabase}.EINA EINA
WHERE
  EINA._deleted_ = 'F'
  AND EINA.MANDT = {Config.MANDT}
 
"""
    )

    return out0
