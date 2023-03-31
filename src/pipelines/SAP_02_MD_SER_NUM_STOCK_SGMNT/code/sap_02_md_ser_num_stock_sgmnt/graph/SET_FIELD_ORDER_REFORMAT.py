from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_02_md_ser_num_stock_sgmnt.config.ConfigStore import *
from sap_02_md_ser_num_stock_sgmnt.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("EQMNT_NUM"), 
        col("STOCK_TYPE_GOODS_MVMT"), 
        col("PLNT"), 
        col("STRG_LOC"), 
        col("BTCH_NUM"), 
        col("SPL_STOCK_IN"), 
        col("SPL_STOCK_CUST_ACCT_NUM"), 
        col("ACCT_NUM_VEND"), 
        col("SLS_ORDR_NUM"), 
        col("ITM_NUM_SLS_ORDR"), 
        col("WBS_ELMNT"), 
        col("OWN_STOCK"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
