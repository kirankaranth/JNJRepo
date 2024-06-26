from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_mvmt_hdr_hmd.config.ConfigStore import *
from sap_md_matl_mvmt_hdr_hmd.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_MVMT_NUM", col("mblnr"))\
        .withColumn("MATL_MVMT_YR", col("mjahr"))\
        .withColumn("EV_TYPE_CD", trim(col("vgart")))\
        .withColumn(
          "MATL_MVMT_DTTM",
          when((col("bldat") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("bldat"), "yyyyMMdd"))
        )\
        .withColumn(
          "PSTNG_DTTM",
          when((col("budat") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("budat"), "yyyyMMdd"))
        )\
        .withColumn("MATL_MVMT_HDR_TXT", trim(col("bktxt")))\
        .withColumn("DOC_TYPE", trim(col("blart")))\
        .withColumn("BILL_OF_LANDG", trim(col("frbnr")))\
        .withColumn(
          "LAST_CHG_DTTM",
          when((col("aedat") == lit("00000000")), lit(None).cast(TimestampType()))\
            .otherwise(to_timestamp(col("aedat"), "yyyyMMdd"))
        )\
        .withColumn("SPLT_GUID_PART1", col("KEY1"))\
        .withColumn("SPLT_GUID_PART2", col("KEY2"))\
        .withColumn("SPLT_GUID_PART3", col("KEY3"))\
        .withColumn("SPLT_GUID_PART4", col("KEY4"))\
        .withColumn("SPLT_GUID_PART5", col("KEY5"))\
        .withColumn("SPLT_GUID_PART6", col("KEY6"))\
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
        .withColumn("_deleted_", lit("F"))\
        .withColumn("MATL_MVMT_ITM_NUM", trim(col("zeile")))\
        .withColumn("DOC_TYPE_DESC", trim(col("ltext")))
