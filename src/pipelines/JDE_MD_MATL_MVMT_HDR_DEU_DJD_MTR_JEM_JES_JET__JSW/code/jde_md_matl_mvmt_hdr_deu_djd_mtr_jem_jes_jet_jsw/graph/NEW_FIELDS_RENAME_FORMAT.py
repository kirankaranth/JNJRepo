from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_deu_djd_mtr_jem_jes_jet_jsw.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_MVMT_NUM", col("ILUKID"))\
        .withColumn("MATL_MVMT_YR", substring((col("ILTRDJ") + lit(1900000)), 1, 4))\
        .withColumn("EV_TYPE_CD", trim(col("ilrcd")))\
        .withColumn(
          "MATL_MVMT_DTTM",
          when(
              (
                ((lower(trim(col("iltrdj"))) == lit("null")) | (trim(col("iltrdj")) == lit("")))
                | (trim(col("iltrdj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("iltrdj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("iltrdj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 4)\
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
          "PSTNG_DTTM",
          when(
              (
                ((lower(trim(col("ildgl"))) == lit("null")) | (trim(col("ildgl")) == lit("")))
                | (trim(col("ildgl")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ildgl")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ildgl")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 4)\
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
        .withColumn("MATL_MVMT_HDR_TXT", trim(col("iltrex")))\
        .withColumn("DOC_TYPE", trim(col("ildct")))\
        .withColumn("DOC_TYPE_DESC", trim(col("drdl01")))\
        .withColumn(
          "LAST_CHG_DTTM",
          when(
              (
                ((lower(trim(col("ilcrdj"))) == lit("null")) | (trim(col("ilcrdj")) == lit("")))
                | (trim(col("ilcrdj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            substring(
              date_add(
                  concat(
                    substring((trim(col("ilcrdj")).cast(IntegerType()) + lit(1900000)).cast(StringType()), 1, 4), 
                    lit("-01-01")
                  ), 
                  (
                    substring((trim(col("ilcrdj")).cast(IntegerType()) + 1900000).cast(StringType()), 5, 4)\
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
          "SPLT_GUID_PART1",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPLT_GUID_PART2",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPLT_GUID_PART3",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPLT_GUID_PART4",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPLT_GUID_PART5",
          lit(
            "#"
          )
        )\
        .withColumn(
          "SPLT_GUID_PART6",
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
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_MVMT_NUM', MATL_MVMT_NUM, 'MATL_MVMT_YR', MATL_MVMT_YR, 'SPLT_GUID_PART1', SPLT_GUID_PART1, 'SPLT_GUID_PART2', SPLT_GUID_PART2, 'SPLT_GUID_PART3', SPLT_GUID_PART3, 'SPLT_GUID_PART4', SPLT_GUID_PART4, 'SPLT_GUID_PART5', SPLT_GUID_PART5, 'SPLT_GUID_PART6', SPLT_GUID_PART6)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MATL_MVMT_NUM', MATL_MVMT_NUM, 'MATL_MVMT_YR', MATL_MVMT_YR, 'SPLT_GUID_PART1', SPLT_GUID_PART1, 'SPLT_GUID_PART2', SPLT_GUID_PART2, 'SPLT_GUID_PART3', SPLT_GUID_PART3, 'SPLT_GUID_PART4', SPLT_GUID_PART4, 'SPLT_GUID_PART5', SPLT_GUID_PART5, 'SPLT_GUID_PART6', SPLT_GUID_PART6)"
              )
            )
          )
        )\
        .withColumn("_l1_upt_", current_timestamp())\
        .withColumn("_deleted_", lit("F"))
