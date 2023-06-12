from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_btch.config.ConfigStore import *
from sap_md_btch.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("SRC_TBL_NM"), 
        col("MATL_NUM"), 
        col("BTCH_NUM"), 
        col("PLNT_CD"), 
        col("SHRT_MATL_NUM")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
