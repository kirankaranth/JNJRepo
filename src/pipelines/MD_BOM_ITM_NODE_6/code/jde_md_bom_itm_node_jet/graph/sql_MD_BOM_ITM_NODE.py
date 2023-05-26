from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_itm_node_jet.config.ConfigStore import *
from jde_md_bom_itm_node_jet.udfs.UDFs import *

def sql_MD_BOM_ITM_NODE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD
, f3002_adt.ixtbm as BOM_CAT_CD
,concat(f3002_adt.ixkit,';',f3002_adt.ixmmcu,';',f3002_adt.ixtbm,';',f3002_adt.ixbqty,';',f3002_adt.ixcoby,';',f3002_adt.ixcpnt,';',f3002_adt.ixsbnt) AS BOM_NUM
,\"01\" AS ALT_BOM_NUM
, f3002_adt.ixitm AS BOM_ITM_NODE_NUM
,concat(f3002_adt.ixcpnt,\".\",f3002_adt.ixsbnt) AS BOM_ITM_NODE_CNTR_NBR
, CASE WHEN LOWER(TRIM(f3002_adt.ixefff)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002_adt.ixefff) = '' OR TRIM(f3002_adt.ixefff) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002_adt.ixefff) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002_adt.ixefff) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS BOM_ITM_VLD_FROM_DTTM
, TRIM(f3002_adt.ixrvno) as CHG_NUM
, CASE WHEN LOWER(TRIM(f3002_adt.ixefff)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002_adt.ixefff) = '' OR TRIM(f3002_adt.ixefff) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002_adt.ixefff) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002_adt.ixefff) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_DTTM
, CASE WHEN LOWER(TRIM(f3002_adt.ixupmj)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002_adt.ixupmj) = '' OR TRIM(f3002_adt.ixupmj) = '0' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(concat(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002_adt.ixupmj) AS INT) + 1900000 as string),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002_adt.ixupmj) AS INT) + 1900000 AS string),5) AS INT )-1) AS string), 1, 10),' ', lpad(TRIM(f3002_adt.ixtday), 6, '0')), \"yyyy-MM-dd HHmmss\") END AS CHG_DTTM
, NULL as DEL_IND,
f3002_adt._upt_ as _l0_upt_,
f3002_adt._deleted_
FROM  {Config.sourceDatabase}.f3002_adt as f3002_adt  WHERE f3002_adt._deleted_ = 'F'
  
 
"""
    )

    return out0
