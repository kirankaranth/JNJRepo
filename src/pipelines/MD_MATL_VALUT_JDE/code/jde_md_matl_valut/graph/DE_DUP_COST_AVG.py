from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def DE_DUP_COST_AVG(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        col("COLEDG"), 
        col("COMCU"), 
        col("COLITM"), 
        col("COITM")
    )

    return df1.agg(mean(col("COUNCS")).alias("COUNCS"), max(col("_upt_")).alias("_upt_"))
