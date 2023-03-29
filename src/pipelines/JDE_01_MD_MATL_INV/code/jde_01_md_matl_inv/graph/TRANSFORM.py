from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def TRANSFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SRC_TBL_NM", lit(Config.DBTABLE1))\
        .withColumn(
          "MATL_NUM",
          coalesce(
            col("IMLITM"), 
            lit(
              "#"
            )
          )
        )\
        .withColumn("PLNT_CD", col("LIMCU"))\
        .withColumn("SLOC_CD", col("LILOCN"))\
        .withColumn("BTCH_NUM", col("LILOTN"))\
        .withColumn("BTCH_STS_CD", col("LILOTS"))\
        .withColumn(
          "SPCL_STCK_IND",
          when(
              ((length(trim(col("lipbin"))) == lit(0)) | col("lipbin").isNull()), 
              lit(
                "#"
              )
            )\
            .otherwise(col("lipbin"))
        )\
        .withColumn(
          "PRTY_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "UNRSTRCTD_STCK",
          when((trim(coalesce(col("LILOTS"), lit(""))) == lit("")), (col("LIPQOH") / lit(Config.DIVISOR)))\
            .otherwise(lit(0))\
            .cast(DecimalType(18, 4))
        )\
        .withColumn("IN_TRNSFR_STCK", (col("LIQTTR") / lit(Config.DIVISOR)).cast(DecimalType(18, 4)))\
        .withColumn("QLTY_INSP_STCK", (col("LIQTIN") / lit(Config.DIVISOR)).cast(DecimalType(18, 4)))\
        .withColumn(
          "RSTRCTD_STCK",
          when((trim(coalesce(col("LILOTS"), lit(""))) != lit("")), (col("LIPQOH") / lit(Config.DIVISOR)))\
            .otherwise(lit(0))\
            .cast(DecimalType(18, 4))
        )\
        .withColumn("BASE_UOM_CD", trim(col("IMUOM1")))\
        .withColumn("SHRT_MATL_NUM", trim(col("LIITM")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
