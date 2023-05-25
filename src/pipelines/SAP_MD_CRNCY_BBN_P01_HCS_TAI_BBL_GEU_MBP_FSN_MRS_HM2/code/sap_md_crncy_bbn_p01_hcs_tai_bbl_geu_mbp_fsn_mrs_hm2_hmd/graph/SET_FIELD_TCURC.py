from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.config.ConfigStore import *
from sap_md_crncy_bbn_p01_hcs_tai_bbl_geu_mbp_fsn_mrs_hm2_hmd.udfs.UDFs import *

def SET_FIELD_TCURC(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(
        col("WAERS"), 
        col("ISOCD"), 
        col("ALTWR"), 
        col("GDATU"), 
        col("XPRIMARY"), 
        col("_upt_"), 
        col("_pk_"), 
        col("_deleted_"), 
        col("MANDT")
    )
