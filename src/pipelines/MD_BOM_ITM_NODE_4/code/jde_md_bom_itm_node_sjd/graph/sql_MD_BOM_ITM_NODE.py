from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_itm_node_sjd.config.ConfigStore import *
from jde_md_bom_itm_node_sjd.udfs.UDFs import *

def sql_MD_BOM_ITM_NODE(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
    SELECT
'{Config.sourceSystem}' AS SRC_SYS_CD
, f3002.ixtbm as BOM_CAT_CD
,concat(f3002.ixkit,';',f3002.ixmmcu,';' ,f3002.ixtbm,';',f3002.ixbqty,';',f3002.ixcoby,';',f3002.ixcpnt,';',f3002.ixsbnt) AS BOM_NUM
, \"01\" AS ALT_BOM_NUM
, f3002.ixitm AS BOM_ITM_NODE_NUM
,concat(f3002.ixcpnt,\".\",f3002.ixsbnt) AS BOM_ITM_NODE_CNTR_NBR
, CASE WHEN LOWER(TRIM(f3002.ixefff)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002.ixefff) = '' OR TRIM(f3002.ixefff) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002.ixefff) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002.ixefff) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS BOM_ITM_VLD_FROM_DTTM
, TRIM(f3002.ixrvno) as CHG_NUM
, CASE WHEN LOWER(TRIM(f3002.ixefff)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002.ixefff) = '' OR TRIM(f3002.ixefff) = '0' THEN NULL ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002.ixefff) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002.ixefff) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_DTTM
, CASE WHEN LOWER(TRIM(f3002.ixupmj)) = 'CAST(NULL AS timestamp)' OR TRIM(f3002.ixupmj) = '' OR TRIM(f3002.ixupmj) = '0' THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(concat(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f3002.ixupmj) AS INT) + 1900000 as string),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f3002.ixupmj) AS INT) + 1900000 AS string),5) AS INT )-1) AS string), 1, 10),' ', lpad(TRIM(f3002.ixtday), 6, '0')), \"yyyy-MM-dd HHmmss\") END AS CHG_DTTM
, NULL as DEL_IND
, f3002._upt_ as _l0_upt_,
f3002._deleted_
FROM  (
SELECT *, row_number() OVER (partition by ixkit, ixmmcu, ixtbm, ixbqty, ixcpnt, ixsbnt order by ixkit, ixmmcu, ixtbm, ixbqty, ixcpnt, ixsbnt) as rank FROM
{Config.sourceDatabase}.f3002 as f3002  WHERE f3002._deleted_ = 'F') f3002
WHERE rank=1
  
 
"""
    )

    return out0
