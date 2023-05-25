from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from MD_CRNCY_TEXT_1.config.ConfigStore import *
from MD_CRNCY_TEXT_1.udfs.UDFs import *

def sql_MD_CRNCY_TEXT(spark: SparkSession) -> DataFrame:
    out0 = spark.sql(
        f"""
SELECT '{Config.sourceSystem}' AS SRC_SYS_CD,
    TCURT.waers AS CRNCY_CD,
    TCURT.spras AS LANG_CD,
    TRIM(TCURT.ktext) AS CRNCY_SHRT_NM,
    TRIM(TCURT.ltext) AS CRCNCY_LONG_NM,
    TCURT._upt_ as _l0_upt_
FROM {Config.sourceDatabase}.TCURT TCURT
WHERE TCURT._deleted_ = 'F'
  AND TCURT.MANDT = {Config.MANDT}
"""
    )

    return out0
