from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_01_md_ship_mtr_bw2_gmd_jes.config.ConfigStore import *
from jde_01_md_ship_mtr_bw2_gmd_jes.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SHIP_NUM", col("SDSHPN"))\
        .withColumn("DELV_TYP_CD", col("SDDCTO"))\
        .withColumn("DOC_REF_NUM", col("SDDOCO"))\
        .withColumn("CO_CD", col("SDKCOO"))\
        .withColumn("DELV_LINE_NBR", col("SDLNID"))\
        .withColumn(
          "ACTL_SHIP_DTTM",
          when(
              (
                ((lower(trim(col("SDADDJ"))) == lit("null")) | (trim(col("SDADDJ")) == lit("")))
                | (trim(col("SDADDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("SDADDJ")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("SDADDJ")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
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
          "PLAN_SHIP_DTTM",
          when(
              (
                ((lower(trim(col("SDPDDJ"))) == lit("null")) | (trim(col("SDPDDJ")) == lit("")))
                | (trim(col("SDPDDJ")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("SDPDDJ")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("SDPDDJ")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 8)\
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
        .withColumn("SLS_ORDR_CAR_CD", trim(col("SDLNID")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())\
        .withColumn("_l0_upt_", col("_upt_"))\
        .withColumn(
          "_pk_",
          to_json(
            expr(
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SHIP_NUM', SHIP_NUM, 'DELV_TYP_CD', DELV_TYP_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'CO_CD', CO_CD, 'DELV_LINE_NBR', DELV_LINE_NBR)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'SHIP_NUM', SHIP_NUM, 'DELV_TYP_CD', DELV_TYP_CD, 'DOC_REF_NUM', DOC_REF_NUM, 'CO_CD', CO_CD, 'DELV_LINE_NBR', DELV_LINE_NBR)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
