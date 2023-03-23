from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.config.ConfigStore import *
from sap_md_bom_hdr_atl_bbl_bwi_fsn_geu_mbp_p01_svs_tai.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("BOM_CAT_CD", col("stlty"))\
        .withColumn("BOM_NUM", col("stlnr"))\
        .withColumn("ALT_BOM_NUM", col("stlal"))\
        .withColumn("BOM_CNTR_NBR", col("stkoz"))\
        .withColumn(
          "BOM_VLD_FROM_DTTM",
          when((col("datuv") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("datuv"), "yyyyMMdd"))
        )\
        .withColumn("CHG_NUM", trim(col("aennr")))\
        .withColumn("PREV_HDR_CNTR_NBR", trim(col("vgkzl")))\
        .withColumn(
          "CRT_DTTM",
          when((col("andat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("andat"), "yyyyMMdd"))
        )\
        .withColumn(
          "CHG_DTTM",
          when((col("aedat") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("aedat"), "yyyyMMdd"))
        )\
        .withColumn("BOM_UOM_CD", trim(col("bmein")))\
        .withColumn("BOM_TXT", trim(col("stktx")))\
        .withColumn("BOM_STS_CD", trim(col("stlst")))\
        .withColumn("DEL_IND", trim(col("loekz")))\
        .withColumn("BOM_BASE_QTY", trim(col("bmeng")).cast(DecimalType(18, 4)))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
