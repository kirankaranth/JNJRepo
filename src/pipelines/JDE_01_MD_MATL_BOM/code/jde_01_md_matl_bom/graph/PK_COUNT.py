from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_bom.config.ConfigStore import *
from jde_01_md_matl_bom.udfs.UDFs import *

def PK_COUNT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("PLNT_CD"), 
        col("BOM_USG_CD"), 
        col("BOM_NUM"), 
        col("ALT_BOM_NUM")
    )

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
