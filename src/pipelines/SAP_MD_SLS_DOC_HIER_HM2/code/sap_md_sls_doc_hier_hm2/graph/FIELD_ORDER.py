from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_hm2.udfs.UDFs import *

def FIELD_ORDER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("SRC_SYS_CD"), 
        col("CO_CD"), 
        col("PREV_DOC_NUM"), 
        col("PREV_DOC_LINE_NBR"), 
        col("SUBSQ_DOC_NUM"), 
        col("SUBSQ_DOC_LINE_NBR"), 
        col("SUBSQ_DOC_CAT_CD"), 
        col("PREV_DOC_TYPE_CD"), 
        col("PREV_DOC_CAT_CD"), 
        col("REF_QTY"), 
        col("BASE_UOM_CD"), 
        col("REF_AMT"), 
        col("CRNCY_CD"), 
        col("CRT_DTTM"), 
        col("MATL_NUM"), 
        col("CHG_DTTM"), 
        col("REQ_TYPE_CD"), 
        col("PLNG_TYPE_CD"), 
        col("LVL_CD"), 
        col("WHSE_CD"), 
        col("BILL_CAT_CD"), 
        col("GRS_WT_MEAS"), 
        col("WT_UOM_CD"), 
        col("VOL_MEAS"), 
        col("VOL_UOM_CD"), 
        col("SLS_UOM_CD"), 
        col("SPL_STK_TYPE_CD"), 
        col("SPL_STK_NUM"), 
        col("NET_WT_MEAS"), 
        col("GM_STS_CD"), 
        col("QTY_CONV_CD"), 
        col("MATL_MVMT_YR"), 
        col("SD_UNIQ_DOC_RL_ID"), 
        col("QTY_CALC_POS_NGTV"), 
        col("ID_MM_WM_TFR_ORDR_CNFRM"), 
        col("MVMT_TYPE"), 
        col("BILL_INVC_PLAN_NUM"), 
        col("ITM_FOR_BILL_INVC_PLAN_PMT_CRD"), 
        col("REF_QTY_SLS_UNIT"), 
        col("REF_QTY_BASE_UNIT_MEAS"), 
        col("GUARNT"), 
        col("IN_INV_MGMT_ACT"), 
        col("LOGL_SYS"), 
        col("DATA_FIL_VAL_DATA_AGE_DTTM"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_md5_"), 
        col("_pk_"), 
        col("_deleted_")
    )
