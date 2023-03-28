from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sk_sap_01_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sk_sap_01_md_ser_num_stock_sgmnt.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("EQUNR").alias("EQMNT_NUM"), 
        trim(col("LBBSA")).alias("STOCK_TYPE_GOODS_MVMT"), 
        trim(col("B_WERK")).alias("PLNT"), 
        trim(col("B_LAGER")).alias("STRG_LOC"), 
        trim(col("B_CHARGE")).alias("BTCH_NUM"), 
        trim(col("KUNNR")).alias("SPL_STOCK_CUST_ACCT_NUM"), 
        trim(col("SOBKZ")).alias("SPL_STOCK_IN"), 
        trim(col("LIFNR")).alias("ACCT_NUM_VEND"), 
        trim(col("KDAUF")).alias("SLS_ORDR_NUM"), 
        trim(col("KDPOS")).alias("ITM_NUM_SLS_ORDR"), 
        trim(col("PS_PSP_PNR")).alias("WBS_ELMNT"), 
        trim(col("DISUB_OWNER")).alias("OWN_STOCK")
    )
