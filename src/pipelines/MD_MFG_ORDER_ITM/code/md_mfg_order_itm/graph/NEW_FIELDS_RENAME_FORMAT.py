from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_mfg_order_itm.config.ConfigStore import *
from md_mfg_order_itm.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(""))\
        .withColumn("MFG_ORDR_TYP_CD", col("AUART"))\
        .withColumn("MFG_ORDR_NUM", col("AUFNR"))\
        .withColumn("LN_ITM_NBR", col("POSNR"))\
        .withColumn("PRDTN_UOM_CD", trim(col("AMEIN")))\
        .withColumn("BTCH_NUM", trim(col("CHARG")))\
        .withColumn("DLV_CMPLT_IND", trim(col("ELIKZ")))\
        .withColumn("MATL_NUM", trim(col("MATNR")))\
        .withColumn("BASE_UOM_CD", trim(col("MEINS")))\
        .withColumn("MFG_PLNND_ORDR_NUM", trim(col("PLNUM")))\
        .withColumn("SCRP_QTY", col("PSAMG").cast(DecimalType(18, 4)))\
        .withColumn("ITM_QTY", col("PSMNG").cast(DecimalType(18, 4)))\
        .withColumn("PLNT_CD", trim(col("PWERK")))\
        .withColumn("FCTR_DNMNTR_MEAS", col("UMREN").cast(DecimalType(18, 4)))\
        .withColumn("FCTR_NMRTR_MEAS", col("UMREZ").cast(DecimalType(18, 4)))\
        .withColumn("PRDNT_VRSN_NUM", trim(col("VERID")))\
        .withColumn("GOOD_RCPT_LD_DAYS_QTY", col("WEBAZ").cast(DecimalType(18, 4)))\
        .withColumn("RCVD_QTY", col("WEMNG").cast(DecimalType(18, 4)))\
        .withColumn("DEL_IND", trim(col("XLOEK")))
