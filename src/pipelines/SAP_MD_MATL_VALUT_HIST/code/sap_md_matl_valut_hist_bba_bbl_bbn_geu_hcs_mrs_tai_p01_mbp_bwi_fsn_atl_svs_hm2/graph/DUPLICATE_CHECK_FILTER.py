from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.config.ConfigStore import *
from sap_md_matl_valut_hist_bba_bbl_bbn_geu_hcs_mrs_tai_p01_mbp_bwi_fsn_atl_svs_hm2.udfs.UDFs import *

def DUPLICATE_CHECK_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
