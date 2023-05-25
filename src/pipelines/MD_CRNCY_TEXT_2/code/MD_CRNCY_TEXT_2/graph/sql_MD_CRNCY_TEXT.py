from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_CRNCY_TEXT_2.config.ConfigStore import *
from MD_CRNCY_TEXT_2.udfs.UDFs import *

def sql_MD_CRNCY_TEXT(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
    F0013.cvcrcd AS CRNCY_CD,
    'E' AS LANG_CD,
    TRIM(F0013.cvdl01) AS CRNCY_SHRT_NM,
    TRIM(F0013.cvdl01) AS CRCNCY_LONG_NM,
    F0013._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.F0013 F0013
WHERE F0013._deleted_ = 'F'  
 
"""
    )

    return out0
