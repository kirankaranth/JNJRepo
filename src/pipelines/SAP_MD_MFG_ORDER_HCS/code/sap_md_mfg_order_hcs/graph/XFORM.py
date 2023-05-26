from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_hcs.config.ConfigStore import *
from sap_md_mfg_order_hcs.udfs.UDFs import *

def XFORM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("MFG_ORDR_TYP_CD", col("AUART"))\
        .withColumn("MFG_ORDR_NUM", col("AUFNR"))\
        .withColumn(
          "CHG_DTTM",
          when((col("AEDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("AEDAT"), "yyyyMMdd"))
        )\
        .withColumn(
          "CRTD_DTTM",
          when((col("ERDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("ERDAT"), "yyyyMMdd"))
        )\
        .withColumn("DEL_IND", trim(col("LOEKZ")))\
        .withColumn("OBJECT_NUMBER", trim(col("OBJNR")))\
        .withColumn("PLNT_CD", trim(col("WERKS")))\
        .withColumn("ORDR_CAT", trim(col("AUTYP")))\
        .withColumn("REF_ORDR_NUM", trim(col("REFNR")))\
        .withColumn("ENT_BY", trim(col("ERNAM")))\
        .withColumn("LAST_CHG_BY", trim(col("AENAM")))\
        .withColumn("DESC", trim(col("KTEXT")))\
        .withColumn("LONG_TEXT_EXISTS", trim(col("LTEXT")))\
        .withColumn("CO_CD", trim(col("BUKRS")))\
        .withColumn("BUSN_AREA", trim(col("GSBER")))\
        .withColumn("CNTL_AREA", trim(col("KOKRS")))\
        .withColumn("COST_CLCT_KEY", trim(col("CCKEY")))\
        .withColumn("ORDR_CRNCY", trim(col("WAERS")))\
        .withColumn("ORDR_STS", trim(col("ASTNR")))\
        .withColumn("LAST_STS_CHG_DTTM", when((col("STDAT") == lit("00000000")), lit(None)).otherwise(to_timestamp(col("STDAT"), "yyyyMMdd")))
