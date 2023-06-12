from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", coalesce(lookup("F4101_LU", col("COITM")).getField("IMLITM"), col("COITM")))\
        .withColumn("VALUT_AREA_CD", col("COMCU"))\
        .withColumn("PRC_CNTL_IND", trim(col("COLEDG")))\
        .withColumn(
          "VALUT_TYPE_CD",
          coalesce(
            col("LIPBIN"), 
            lit(
              "#"
            )
          )
        )\
        .withColumn(
          "TOT_STK_QTY",
          when(
              ((col("TOT_INV") / lit(Config.divisor)) + (col("INSP") / lit(Config.divisor))).isNull(), 
              lit(0).cast(DecimalType(18, 4))
            )\
            .otherwise(((col("TOT_INV") / lit(Config.divisor)) + (col("INSP") / lit(Config.divisor))).cast(DecimalType(18, 4)))
        )\
        .withColumn("VALUT_CLS_CD", trim(lookup("F4101_LU", col("LIITM")).getField("IMGLPT")))\
        .withColumn("BASE_UOM_CD", trim(lookup("F4101_LU", col("LIITM")).getField("IMUOM1")))\
        .withColumn("PRC_AMT", col("COUNCS").cast(DecimalType(18, 4)))\
        .withColumn("PRC_UNIT_NBR", lit(Config.costDivisor).cast(DecimalType(18, 4)))\
        .withColumn(
          "TOT_VAL_AMT",
          when(
              (
                (
                  col("COUNCS")
                  / lit(Config.costDivisor)
                )
                * (
                  (col("TOT_INV") / lit(Config.divisor))
                  + (col("INSP") / lit(Config.divisor))
                )
              )\
                .isNull(
              ), 
              lit(0).cast(DecimalType(18, 4))
            )\
            .otherwise((
            (
              col("COUNCS")
              / lit(Config.costDivisor)
            )
            * (
              (col("TOT_INV") / lit(Config.divisor))
              + (col("INSP") / lit(Config.divisor))
            )
          )\
            .cast(
            DecimalType(18, 4)
          ))
        )\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'VALUT_AREA_CD', VALUT_AREA_CD, 'VALUT_TYPE_CD', VALUT_TYPE_CD)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'VALUT_AREA_CD', VALUT_AREA_CD, 'VALUT_TYPE_CD', VALUT_TYPE_CD)"
              )
            )
          )
        )\
        .withColumn("_deleted_", lit("F"))
