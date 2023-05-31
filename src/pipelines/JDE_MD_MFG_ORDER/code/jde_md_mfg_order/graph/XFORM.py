from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_mfg_order.config.ConfigStore import *
from jde_md_mfg_order.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("WADCTO"))\
        .withColumn("MFG_ORDR_NUM", col("WADOCO"))\
        .withColumn("CNFRMD_SCRP_QTY", col("WASOCN").cast(DecimalType(18, 4)))\
        .withColumn("RTNG_TYP_CD", trim(col("WATRT")))\
        .withColumn("MFG_ORDR_STTS_CD", trim(col("WASRST")))\
        .withColumn("BOM_CAT_CD", trim(col("watbm")))\
        .withColumn("PLNT_CD", trim(col("wamcu")))\
        .withColumn("DIR_SHIP_CUST_ADDR_NUM", trim(col("waan8")))\
        .withColumn("MFR_OF_FNL_SKU", trim(col("wammcu")))\
        .withColumn(
          "CHG_DTTM",
          to_timestamp(
            date_add(
              substring((col("waupmj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("waupmj"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "CRTD_DTTM",
          to_timestamp(
            date_add(
              substring((col("watrdj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("watrdj"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "ACT_RLSE_DTTM",
          to_timestamp(
            date_add(
              substring((col("wappdt") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wappdt"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PLAN_RLSE_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrt") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrt"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SCH_REL_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrt") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrt"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "CNFRMD_END_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrx") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrx"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "PRDTN_END_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrx") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrx"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "END_DTTM",
          to_timestamp(
            date_add(
              substring((col("wadrqj") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wadrqj"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SCHD_END_DTTM",
          to_timestamp(
            date_add(
              substring((col("wadpl") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wadpl"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "STRT_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrt") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrt"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SCHD_STRT_DTTM",
          to_timestamp(
            date_add(
              substring((col("wastrt") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("wastrt"), 4, 3).cast(IntegerType()) - 1)
            )
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
              "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
            )
          )
        )\
        .withColumn(
          "_pk_md5_",
          md5(
            to_json(
              expr(
                "named_struct('SRC_SYS_CD', SRC_SYS_CD, 'MFG_ORDR_TYP_CD', MFG_ORDR_TYP_CD, 'MFG_ORDR_NUM', MFG_ORDR_NUM)"
              )
            )
          )
        )\
        .withColumn("_1l_upt_", current_timestamp())\
        .withColumn("_l0_deleted_", col("_deleted_"))
