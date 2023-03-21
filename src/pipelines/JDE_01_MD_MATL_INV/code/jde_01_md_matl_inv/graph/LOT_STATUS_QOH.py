from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def LOT_STATUS_QOH(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "XFRM_SPCL_STCK_IND",
          when(
              ((length(trim(col("lipbin"))) == lit(0)) | col("lipbin").isNull()), 
              lit(
                "#"
              )
            )\
            .otherwise(col("lipbin"))
        )\
        .withColumn(
          "XFRM_PRTY_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "XFRM_UNRSTRCTD_STCK",
          when((trim(coalesce(col("LILOTS"), lit(""))) == lit("")), col("LIPQOH"))\
            .otherwise(lit(0))\
            .alias("UNRSTRCTD_STCK")
        )\
        .withColumn("XFRM_RSTRCTD_STCK", when((trim(coalesce(col("LILOTS"), lit(""))) != lit("")), col("LIPQOH"))\
        .otherwise(lit(0))\
        .alias("RSTRCTD_STCK"))
