from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.config.ConfigStore import *
from sap_md_bom_hdr_atl_bba_bbl_bbn_bwi_fsn_geu_hcs_mbp_mrs_p01_svs_tai_hmd.udfs.UDFs import *

def PK_GT_1(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
