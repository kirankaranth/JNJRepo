from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.config.ConfigStore import *
from sap_md_matl_bom_atl_bba_bbl_bbn_hcs_mbp_mrs_p01_svs_bwi_fsn_tai_geu_hmd.udfs.UDFs import *

def SAP_MAST(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mast")
