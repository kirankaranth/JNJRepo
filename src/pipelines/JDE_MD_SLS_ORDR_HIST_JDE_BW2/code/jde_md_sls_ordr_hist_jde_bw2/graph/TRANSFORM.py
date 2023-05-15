from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_hist_jde_bw2.config.ConfigStore import *
from jde_md_sls_ordr_hist_jde_bw2.udfs.UDFs import *

def TRANSFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("ORDR_TYPE", col("SLDCTO"))\
        .withColumn(
          "UPDT_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLUPMJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLUPMJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("TIME_OF_DAY", col("SLTDAY"))\
        .withColumn("LINE_NUM", col("SLLNID"))\
        .withColumn("ORDR_CO", col("SLKCOO"))\
        .withColumn("DOC_NUM", col("SLDOCO"))\
        .withColumn("ORDR_SFX", trim(col("SLSFXO")))\
        .withColumn("BUSN_UNIT", trim(col("SLMCU")))\
        .withColumn("CO", trim(col("SLCO")))\
        .withColumn("DOC_CO_ORIG_ORDR", trim(col("SLOKCO")))\
        .withColumn("ORIG_ORDR_NUM", trim(col("SLOORN")))\
        .withColumn("ORIG_ORDR_TYPE", trim(col("SLOCTO")))\
        .withColumn("ORIG_LINE_NUM", trim(col("SLOGNO")))\
        .withColumn("COMPANY_KEY", trim(col("SLRKCO")))\
        .withColumn("RLTD_PO_NUM", trim(col("SLRORN")))\
        .withColumn("RLTD_PO_ORDR_TYPE", trim(col("SLRCTO")))\
        .withColumn("RLTD_PO_LINE_NUM", trim(col("SLRLLN")))\
        .withColumn("AGMT_NUM_DSTN", trim(col("SLDMCT")))\
        .withColumn("AGMT_SPLMN_DSTN", trim(col("SLDMCS")))\
        .withColumn("ADDR_NUM", trim(col("SLAN8")))\
        .withColumn("ADDR_NUM_SHIP_TO", trim(col("SLSHAN")))\
        .withColumn("ADDR_NUM_PARNT", trim(col("SLPA8")))\
        .withColumn(
          "RQST_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLDRQJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLDRQJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "ORDR_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLTRDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLTRDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SCHD_PICK_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLPDDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPDDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "ACTL_SHIP_DTTM",
          to_timestamp(
            date_add(
              substring((col("SLADDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLADDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLIVD",
          to_timestamp(
            date_add(
              substring((col("SLIVD") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLIVD"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLCNDJ",
          to_timestamp(
            date_add(
              substring((col("SLCNDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLCNDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLDGL",
          to_timestamp(
            date_add(
              substring((col("SLDGL") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLDGL"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLRSDJ",
          to_timestamp(
            date_add(
              substring((col("SLRSDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLRSDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLPEFJ",
          to_timestamp(
            date_add(
              substring((col("SLPEFJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPEFJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn(
          "SLPPDJ",
          to_timestamp(
            date_add(
              substring((col("SLPPDJ") + lit(1900000)), 1, 4).cast(DateType()), 
              (substring(col("SLPPDJ"), 4, 3).cast(IntegerType()) - 1)
            )
          )
        )\
        .withColumn("SLVR01", trim(col("SLVR01")))
