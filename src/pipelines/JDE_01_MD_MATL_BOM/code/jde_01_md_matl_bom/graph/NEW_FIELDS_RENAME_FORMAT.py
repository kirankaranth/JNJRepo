from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_bom.config.ConfigStore import *
from jde_01_md_matl_bom.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_NUM", col("ixkitl"))\
        .withColumn("PLNT_CD", col("ixmmcu"))\
        .withColumn("BOM_USG_CD", col("ixtbm"))\
        .withColumn(
          "BOM_NUM",
          concat(
            col("ixkit"), 
            lit(";"), 
            col("ixmmcu"), 
            lit(";"), 
            col("ixtbm"), 
            lit(";"), 
            col("ixbqty"), 
            lit(";"), 
            col("ixcoby"), 
            lit(";"), 
            col("ixcpnt"), 
            lit(";"), 
            col("ixsbnt")
          )
        )\
        .withColumn("ALT_BOM_NUM", lit("01").cast(StringType()))\
        .withColumn(
          "CRT_DTTM",
          when(
              (
                ((lower(trim(col("ixefff"))) == lit("null")) | (trim(col("ixefff")) == lit("")))
                | (trim(col("ixefff")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ixefff")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ixefff")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                      .cast(IntegerType())
                    - 1
                  )
                )\
                .cast(StringType()), 
              1, 
              10
            ), 
            "yyyy-MM-dd"
          ))
        )\
        .withColumn(
          "CHG_DTTM",
          when(
              (
                ((lower(trim(col("ixupmj"))) == lit("null")) | (trim(col("ixupmj")) == lit("")))
                | (trim(col("ixupmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            concat(
              substring(
                date_add(
                    concat(
                      substring((trim(col("ixupmj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                      lit("-01-01")
                    ), 
                    (
                      substring((trim(col("ixupmj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
                        .cast(IntegerType())
                      - 1
                    )
                  )\
                  .cast(StringType()), 
                1, 
                10
              ), 
              lit(" "), 
              lpad(trim(col("ixtday")), 6, "0")
            ), 
            "yyyy-MM-dd HHmmss"
          ))
        )\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'BOM_USG_CD', BOM_USG_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_NUM', MATL_NUM, 'PLNT_CD', PLNT_CD, 'BOM_USG_CD', BOM_USG_CD, 'BOM_NUM', BOM_NUM, 'ALT_BOM_NUM', ALT_BOM_NUM)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
