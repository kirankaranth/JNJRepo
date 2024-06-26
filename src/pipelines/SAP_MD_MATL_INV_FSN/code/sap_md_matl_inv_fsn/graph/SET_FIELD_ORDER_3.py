from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_fsn.config.ConfigStore import *
from sap_md_matl_inv_fsn.udfs.UDFs import *

def SET_FIELD_ORDER_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("SRC_TBL_NM"), 
        col("MATL_NUM"), 
        col("PLNT_CD"), 
        col("SLOC_CD"), 
        col("BTCH_NUM"), 
        col("BTCH_STS_CD"), 
        col("SPCL_STCK_IND"), 
        col("PRTY_NUM"), 
        col("UNRSTRCTD_STCK"), 
        col("IN_TRNSFR_STCK"), 
        col("QLTY_INSP_STCK"), 
        col("RSTRCTD_STCK"), 
        col("BLCKD_STCK"), 
        col("CRT_DTTM"), 
        col("RTRNS"), 
        col("BASE_UOM_CD"), 
        col("STO_IN_TRNST_QTY"), 
        col("PLNT_IN_TRNST_QTY"), 
        col("FISC_YR_OF_CUR_PER"), 
        col("CUR_PER"), 
        col("SHRT_MATL_NUM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_deleted_")
    )
