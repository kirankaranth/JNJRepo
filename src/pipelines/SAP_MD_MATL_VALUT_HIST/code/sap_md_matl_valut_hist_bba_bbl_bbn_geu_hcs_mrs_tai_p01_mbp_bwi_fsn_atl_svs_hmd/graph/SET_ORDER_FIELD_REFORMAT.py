from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hmd.udfs.UDFs import *

def SET_ORDER_FIELD_REFORMAT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("MATL_NUM"), 
        col("VALUT_AREA_CD"), 
        col("VALUT_TYPE_CD"), 
        col("PSTNG_YR"), 
        col("PSTNG_PER"), 
        col("PRC_CNTL_IND"), 
        col("TOT_STK_QTY"), 
        col("TOT_VAL_AMT"), 
        col("MVG_AVG_PRC_AMT"), 
        col("PRC_AMT"), 
        col("PRC_UNIT_NBR"), 
        col("VALUT_CLS_CD"), 
        col("BASE_UOM_CD"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_pk_"), 
        col("_pk_md5_"), 
        col("_l1_upt_"), 
        col("_deleted_")
    )
