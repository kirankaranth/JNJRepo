from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.config.ConfigStore import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.udfs.UDFs import *

def Filter_SAP_01_MAST(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(((col("MANDT") == lit(Config.MANDT)) & (col("_deleted_") == lit("F"))))
