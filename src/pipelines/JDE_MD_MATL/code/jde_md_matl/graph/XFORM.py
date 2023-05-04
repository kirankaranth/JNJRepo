from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl.config.ConfigStore import *
from jde_md_matl.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("MATL_NUM", col("IMLITM"))\
        .withColumn("BASE_UOM_CD", trim(col("IMUOM1")))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", col("IMSLD").cast(DecimalType(18, 4)))\
        .withColumn("MATL_STS_CD", trim(col("IMSTKT")))\
        .withColumn("MATL_DOC_NUM", trim(col("IMDRAW")))\
        .withColumn("MATL_DOC_VERS_NUM", trim(col("IMRVNO")))\
        .withColumn("MATL_CATLG_NUM", trim(col("IMAITM")))\
        .withColumn("CHG_BY", trim(col("IMUSER")))\
        .withColumn("WT_UNIT", trim(col("IMUWUM")))\
        .withColumn("DOC_TYPE", trim(col("IMLNTY")))\
        .withColumn("SHRT_MATL_NUM", trim(col("IMITM")))\
        .withColumn(
          "LAST_CHG_DT_TIME_DTTM",
          when(
              (length(col("imtday")) == lit(6)), 
              to_timestamp(
                concat(
                  date_add(
                    substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
                    (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - lit(1))
                  ), 
                  lit(""), 
                  col("imtday")
                ), 
                "yyyy-MM-ddHHmmss"
              )
            )\
            .when(
              (length(col("imtday")) == lit(5)), 
              to_timestamp(
                concat(
                  date_add(
                    substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
                    (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - lit(1))
                  ), 
                  lit(""), 
                  concat(lit(0), col("imtday"))
                ), 
                "yyyy-MM-ddHHmmss"
              )
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("IMUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("IMUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("SER_LVL", trim(col("IMMPST")))\
        .withColumn("SER_TYPE", trim(col("IMPTSC")))\
        .withColumn("BRAVO_MINOR_CODE_DESC", lookup("BRAVO_D_LU", trim(col("B_M_LU"))).getField("DRDL01"))\
        .withColumn("FRAN_CD_DESC", lookup("FRAN_LU", trim(col("F_C_LU"))).getField("DRDL01"))\
        .withColumn("MATL_GRP_DESC", lookup("MATL_GR_LU", trim(col("M_G_LU"))).getField("DRDL01"))\
        .withColumn("MATL_TYPE_DESC", lookup("MATL_T_LU", trim(col("M_T_D_LU"))).getField("DRDL01"))\
        .withColumn("MATL_TYPE_CD", expr(Config.MATL_TYPE_CD).cast(StringType()))
