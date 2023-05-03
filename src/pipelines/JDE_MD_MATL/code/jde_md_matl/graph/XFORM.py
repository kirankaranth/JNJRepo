from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl.config.ConfigStore import *
from jde_md_matl.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("MATL_NUM", col("IMLITM"))\
        .withColumn("BASE_UOM_CD", col("IMUOM1"))\
        .withColumn("TOT_SHLF_LIF_DAYS_CNT", col("IMSLD"))\
        .withColumn("MATL_STS_CD", col("IMSTKT"))\
        .withColumn("MTCH_MNG_IND", col("IMSRCE"))\
        .withColumn("MATL_DOC_NUM", col("IMDRAW"))\
        .withColumn("MATL_DOC_VERS_NUM", col("IMRVNO"))\
        .withColumn("MATL_CATLG_NUM", col("IMAITM"))\
        .withColumn("CHG_BY", col("IMUSER"))\
        .withColumn("WT_UNIT", col("IMUWUM"))\
        .withColumn("DOC_TYPE", col("IMLNTY"))\
        .withColumn("SHRT_MATL_NUM", col("IMITM"))\
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
        .withColumn("SER_LVL", col("IMMPST"))\
        .withColumn("SER_TYPE", col("IMPTSC"))
