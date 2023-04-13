from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_mvmt_hdr.config.ConfigStore import *
from sap_md_matl_mvmt_hdr.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MATL_MVMT_NUM", col("ILUKID"))\
        .withColumn("MATL_MVMT_YR", substring((col("ILTRDJ") + lit(1900000)), 1, 4))\
        .withColumn("EV_TYPE_CD", trim(col("ilrcd")))\
        .withColumn(
          "MATL_MVMT_DTTM",
          to_timestamp(
            date_add(
              substring((col("iltrdj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("iltrdj"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PSTNG_DTTM",
          to_timestamp(
            date_add(
              substring((col("ildgl") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("ildgl"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("MATL_MVMT_HDR_TXT", trim(col("iltrex")))\
        .withColumn("DOC_TYPE", trim(col("ildct")))\
        .withColumn("DOC_TYPE_DESC", trim(col("drdl01")))\
        .withColumn(
          "LAST_CHG_DTTM",
          to_timestamp(
            date_add(
              substring((col("ilcrdj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("ilcrdj"), 4, 3).cast(IntegerType()) - 1)
            )
          )
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
