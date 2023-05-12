from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.config.ConfigStore import *
from jde_01_md_btch_bw2_gmd_jet_jsw_deu_mtr_sjd_djd_jem_jes.udfs.UDFs import *

def NEW_FIELDS(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("SRC_TBL_NM", lit(Config.sourceTable))\
        .withColumn("MATL_NUM", col("IOLITM"))\
        .withColumn("BTCH_NUM", col("IOLOTN"))\
        .withColumn("PLNT_CD", col("IOMCU"))\
        .withColumn("CRT_DTTM", expr(Config.CRT_DTTM))\
        .withColumn("CRT_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("AVAIL_DTTM", expr(Config.AVAIL_DTTM))\
        .withColumn(
          "CHG_DTTM",
          when(
              (
                (lower(trim(col("ioupmj"))).isNull() | (trim(col("ioupmj")) == lit("")))
                | (trim(col("ioupmj")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("ioupmj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("ioupmj"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("AVAIL_DTTM", lit(None).cast(TimestampType()))\
        .withColumn(
          "BTCH_EXP_DTTM",
          when(
              (
                (lower(trim(col("iommej"))).isNull() | (trim(col("iommej")) == lit("")))
                | (trim(col("iommej")) == lit("0"))
              ), 
              lit(None)
            )\
            .otherwise(to_timestamp(
            date_add(
              substring((col("iommej") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("iommej"), 4, 3).cast(IntegerType()) - 1)
            )
          ))
        )\
        .withColumn("BTCH_STS_CD", trim(col("IOLOTS")))\
        .withColumn("BTCH_LAST_STS_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("SUP_NUM", trim(col("IOVEND")))\
        .withColumn("SUP_BTCH_NUM", trim(col("IORLOT")))\
        .withColumn("BTCH_LAST_GR_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("BTCH_MFG_DTTM", lit(None).cast(TimestampType()))\
        .withColumn("BTCH_TYPE", trim(col("IODCTO")))\
        .withColumn("SUI_IND", expr(Config.SUI_IND))\
        .withColumn("LOT_GRADE", trim(col("IOLOTG")))\
        .withColumn("SHRT_MATL_NUM", trim(col("IOITM")))\
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
