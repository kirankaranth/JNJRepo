from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist.config.ConfigStore import *
from sap_md_matl_valut_hist.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("VALUT_AREA_CD"), 
        col("VALUT_TYPE_CD"), 
        col("PSTNG_YR"), 
        col("PSTNG_PER")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
