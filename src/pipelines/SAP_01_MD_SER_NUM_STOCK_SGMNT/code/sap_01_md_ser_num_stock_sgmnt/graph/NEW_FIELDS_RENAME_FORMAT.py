from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def NEW_FIELDS_RENAME_FORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn("SRC_SYS_CD", lit(Config.sourceSystem))\
        .withColumn("EQMNT_NUM", col("EQUNR"))\
        .withColumn("STOCK_TYPE_GOODS_MVMT", trim(col("LBBSA")))\
        .withColumn("PLNT", trim(col("B_WERK")))\
        .withColumn("STRG_LOC", trim(col("B_LAGER")))\
        .withColumn("BTCH_NUM", trim(col("B_CHARGE")))\
        .withColumn("SPL_STOCK_IN", trim(col("SOBKZ")))\
        .withColumn("SPL_STOCK_CUST_ACCT_NUM", trim(col("KUNNR")))\
        .withColumn("ACCT_NUM_VEND", trim(col("LIFNR")))\
        .withColumn("SLS_ORDR_NUM", trim(col("KDAUF")))\
        .withColumn("ITM_NUM_SLS_ORDR", trim(col("KDPOS")))\
        .withColumn("WBS_ELMNT", trim(col("PS_PSP_PNR")))\
        .withColumn("OWN_STOCK", trim(col("DISUB_OWNER")))\
        .withColumn("DAI_ETL_ID", lit(Config.DAI_ETL_ID))\
        .withColumn("DAI_CRT_DTTM", current_timestamp())\
        .withColumn("DAI_UPDT_DTTM", current_timestamp())
