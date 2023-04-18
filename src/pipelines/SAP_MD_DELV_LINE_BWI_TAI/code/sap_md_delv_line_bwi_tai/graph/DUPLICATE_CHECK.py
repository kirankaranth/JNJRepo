from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bwi_tai.config.ConfigStore import *
from sap_md_delv_line_bwi_tai.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("DELV_NUM"), 
        col("DELV_LINE_NBR"), 
        col("DELV_TYP_CD"), 
        col("CO_CD"), 
        col("DOC_REF_NUM"), 
        col("ORDR_SFX"), 
        col("MATCH_TYPE"), 
        col("SRC_TBL_NM")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
