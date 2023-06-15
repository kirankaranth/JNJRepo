from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn_hm2.udfs.UDFs import *

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
        col("MATL_NUM")
    )
