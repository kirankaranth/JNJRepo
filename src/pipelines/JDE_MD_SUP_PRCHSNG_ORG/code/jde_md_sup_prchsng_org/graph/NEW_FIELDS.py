from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sup_prchsng_org.config.ConfigStore import *
from jde_md_sup_prchsng_org.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SUP_NUM", col("A6AN8"))\
        .withColumn(
          "PRCHSNG_ORG_NUM",
          lit(
            "#"
          )
        )\
        .withColumn(
          "CRT_ON_DTTM",
          when(
              (
                (lower(trim(col("a6upmj"))).isNull() | (trim(col("a6upmj")) == lit("")))
                | (trim(col("a6upmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("a6upmj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("a6upmj"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("CRNCY_CD", trim(col("A6CRRP")))\
        .withColumn("INCOTERM1_CD", trim(col("A6FRTH")))\
        .withColumn("INCOTERM2_CD", trim(col("ABMCU")))\
        .withColumn("EVAL_RCPT_SETLM_CD", trim(col("A6AVCH")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn(
          "_pk_",
          to_json(
            expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SUP_NUM', SUP_NUM, 'PRCHSNG_ORG_NUM', PRCHSNG_ORG_NUM)")
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr("named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SUP_NUM', SUP_NUM, 'PRCHSNG_ORG_NUM', PRCHSNG_ORG_NUM)")
            )
          )
        )\
        .withColumn("_deleted_", col("_deleted_"))
