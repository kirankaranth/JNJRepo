from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_co_jet.config.ConfigStore import *
from jde_md_sup_co_jet.udfs.UDFs import *

def sql_MD_SUP_CO(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
f0401.a6an8 AS SUP_NUM,
f0101_adt.abmcu AS CO_CD,
CASE WHEN LOWER(TRIM(f0401.a6upmj)) = 'null' OR TRIM(f0401.a6upmj) = '' OR TRIM(f0401.a6upmj) = '0'
THEN CAST(NULL AS TIMESTAMP) ELSE to_timestamp(substr(CAST(date_add(concat(substr(CAST(CAST(TRIM(f0401.a6upmj) AS INT) + 1900000 AS STRING),1,4),'-01-01'), CAST(substr(CAST(CAST(TRIM(f0401.a6upmj) AS INT) + 1900000 AS string),5) AS INT )-1) AS STRING), 1, 10),'yyyy-MM-dd') END AS CRT_ON_DTTM,
NULL AS PSTNG_BLK_IND,
NULL AS DEL_IND,
NULL AS LDGR_ACCT_CD,
trim(f0401.a6pyin) AS PMT_METH_CD,
trim(f0401.a6hdpy) AS PMT_BLK_IND,
trim(f0401.a6trap) AS PMT_TERM_CD,
NULL AS BLOK_SUP_IND,
NULL AS OWN_EXPLN_OF_TERM_OF_PMT,
f0401._upt_ as _l0_upt_,
f0401._deleted_
FROM {Config.sourceDatabase}.f0401 f0401
left join {Config.sourceDatabase}.f0101_adt f0101_adt on f0401.a6an8=f0101_adt.aban8 and f0101_adt._deleted_ = 'F'
WHERE f0401._deleted_ = 'F'  
 
"""
    )

    return out0
