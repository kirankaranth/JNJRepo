from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.config.ConfigStore import *
from sap_md_sls_doc_hier_bba_bbl_bbn_mbp_hcs_geu_mrs_tai_svs_bwi_p01_atl_fsn.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(
        ["SRC_SYS_CD",  "CO_CD",  "PREV_DOC_NUM",  "PREV_DOC_LINE_NBR",  "SUBSQ_DOC_NUM",  "SUBSQ_DOC_LINE_NBR",  "SUBSQ_DOC_CAT_CD",          "PREV_DOC_TYPE_CD",  "PREV_DOC_CAT_CD",  "SD_UNIQ_DOC_RL_ID"]
    )
