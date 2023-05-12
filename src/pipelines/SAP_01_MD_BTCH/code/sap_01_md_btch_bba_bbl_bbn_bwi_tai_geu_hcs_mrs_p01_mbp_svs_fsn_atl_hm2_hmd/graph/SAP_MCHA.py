from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.config.ConfigStore import *
from sap_01_md_btch_bba_bbl_bbn_bwi_tai_geu_hcs_mrs_p01_mbp_svs_fsn_atl_hm2_hmd.udfs.UDFs import *

def SAP_MCHA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mcha")
