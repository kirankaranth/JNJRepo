from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch.config.ConfigStore import *
from sap_01_md_btch.udfs.UDFs import *

def SchemaTransform_MCH1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SRC_TBL_NM", lit(Config.DBTable2))\
        .withColumn("MATL_NUM", col("MATNR"))\
        .withColumn("BTCH_NUM", col("CHARG"))\
        .withColumn(
          "PLNT_CD",
          lit(
            "#"
          )
        )\
        .withColumn("DEL_IND", trim(col("LVORM")))\
        .withColumn(
          "CRT_DTTM",
          when((col("ersda") == lit("00000000")), lit(None))\
            .when((length(col("ersda")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("ersda"), "yyyyMMdd"))
        )\
        .withColumn(
          "CHG_DTTM",
          when((col("laeda") == lit("00000000")), lit(None))\
            .when((length(col("laeda")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("laeda"), "yyyyMMdd"))
        )\
        .withColumn(
          "AVAIL_DTTM",
          when((col("verab") == lit("00000000")), lit(None))\
            .when((length(col("verab")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("verab"), "yyyyMMdd"))
        )\
        .withColumn(
          "BTCH_EXP_DTTM",
          when((col("vfdat") == lit("00000000")), lit(None))\
            .when((length(col("vfdat")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("vfdat"), "yyyyMMdd"))
        )\
        .withColumn("BTCH_STS_CD", trim(col("ZUSTD")))\
        .withColumn(
          "BTCH_LAST_STS_DTTM",
          when((col("zaedt") == lit("00000000")), lit(None))\
            .when((length(col("zaedt")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("zaedt"), "yyyyMMdd"))
        )\
        .withColumn("SUP_NUM", trim(col("LIFNR")))\
        .withColumn("SUP_BTCH_NUM", trim(col("LICHA")))\
        .withColumn(
          "BTCH_LAST_GR_DTTM",
          when((col("lwedt") == lit("00000000")), lit(None))\
            .when((length(col("lwedt")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("lwedt"), "yyyyMMdd"))
        )\
        .withColumn(
          "BTCH_MFG_DTTM",
          when((col("hsdat") == lit("00000000")), lit(None))\
            .when((length(col("hsdat")) < lit(8)), lit(None))\
            .otherwise(to_timestamp(col("hsdat"), "yyyyMMdd"))
        )\
        .withColumn("BTCH_TYPE", expr(Config.BTCH_TYPE))\
        .withColumn("SUI_IND", lit(None).cast(StringType()))\
        .withColumn("LOT_GRADE", lit(None).cast(StringType()))\
        .withColumn("PARENT_CODE", lit(None).cast(StringType()))\
        .withColumn(
          "SHRT_MATL_NUM",
          lit(
            "#"
          )
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SRC_TBL_NM', SRC_TBL_NM, 'MATL_NUM', MATL_NUM, 'BTCH_NUM', BTCH_NUM, 'PLNT_CD', PLNT_CD, 'SHRT_MATL_NUM', SHRT_MATL_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SRC_TBL_NM', SRC_TBL_NM, 'MATL_NUM', MATL_NUM, 'BTCH_NUM', BTCH_NUM, 'PLNT_CD', PLNT_CD, 'SHRT_MATL_NUM', SHRT_MATL_NUM)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
