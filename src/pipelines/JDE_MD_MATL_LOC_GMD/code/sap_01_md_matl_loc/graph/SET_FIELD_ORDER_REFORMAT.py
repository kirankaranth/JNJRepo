from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def SET_FIELD_ORDER_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        lit(Config.sourceSystem).alias("SRC_SYS_CD"), 
        col("iblitm").alias("MATL_NUM"), 
        col("ibmcu").alias("PLNT_CD"), 
        trim(col("ibstkt")).alias("SPEC_MATL_PLNT_STS_CD"), 
        trim(col("ibopc")).alias("LOT_SIZE_VAL"), 
        trim(col("ibanpl")).alias("MATL_PLNR_NUM"), 
        trim(col("ibabcs")).alias("MATL_ABC_CLSN_CD"), 
        trim(col("ibckav")).alias("AVLBLTY_CHK_IND"), 
        trim(col("ibstkt")).alias("PRCMT_TYPE_CD"), 
        trim(col("ibsrp6")).alias("MRP_TYPE_CD"), 
        trim(col("iborig")).alias("ORIG_CTRY_CD"), 
        trim(col("ibmult")).cast(DecimalType(18, 4)).alias("RD_VAL_QTY"), 
        trim(col("ibopv")).cast(DecimalType(18, 4)).alias("LOT_SIZE_FX_QTY"), 
        trim(col("ibrqmx")).cast(DecimalType(22, 4)).alias("LOT_SIZE_MAX_QTY"), 
        trim(col("ibrqmn")).cast(DecimalType(18, 4)).alias("LOT_SIZE_MIN_QTY"), 
        trim(col("ibsafe")).cast(DecimalType(18, 4)).alias("SFTY_STK_QTY"), 
        trim(col("ibltlv")).alias("PLNG_TIME_FENCE_DAYS_CNT"), 
        trim(col("ibltlv")).cast(DecimalType(18, 4)).alias("PLAN_DELV_DAYS_CNT"), 
        trim(col("ibltlv")).cast(DecimalType(18, 4)).alias("INHS_PRDTN_DAYS_CNT"), 
        trim(col("ibvend")).alias("PRCHSNG_GRP_CD"), 
        trim(col("ibsrp7")).alias("MSTR_PLNG_FMLY_CD"), 
        trim(col("ibglpt")).alias("VALUT_CAT"), 
        trim(col("ibaitm")).alias("FLLP_MATL"), 
        trim(col("ibsrp5")).alias("CNTL_CODE"), 
        trim(col("ibprp3")).alias("MM_DFLT_SUPP_AREA"), 
        trim(col("ibprp2")).alias("MTS_MTO_FL"), 
        lit(Config.DAI_ETL_ID).alias("DAI_ETL_ID"), 
        current_timestamp().alias("DAI_CRT_DTTM"), 
        current_timestamp().alias("DAI_UPDT_DTTM")
    )
