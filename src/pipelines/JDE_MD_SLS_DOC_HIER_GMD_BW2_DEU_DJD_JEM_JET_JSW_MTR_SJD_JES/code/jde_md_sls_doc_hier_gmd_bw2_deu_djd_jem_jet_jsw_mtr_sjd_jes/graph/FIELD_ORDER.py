from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.config.ConfigStore import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.udfs.UDFs import *

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
        col("BASE_UOM_CD"), 
        col("CRNCY_CD"), 
        col("CRT_DTTM"), 
        col("MATL_NUM"), 
        col("CHG_DTTM"), 
        col("GRS_WT_MEAS"), 
        col("WT_UOM_CD"), 
        col("VOL_MEAS"), 
        col("VOL_UOM_CD"), 
        col("SLS_UOM_CD"), 
        col("NET_WT_MEAS"), 
        col("MATL_MVMT_YR"), 
        col("SD_UNIQ_DOC_RL_ID"), 
        col("DAI_ETL_ID"), 
        col("DAI_CRT_DTTM"), 
        col("DAI_UPDT_DTTM"), 
        col("_l0_upt_"), 
        col("_l1_upt_"), 
        col("_pk_md5_"), 
        col("_pk_"), 
        col("_deleted_")
    )
