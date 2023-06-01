from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.config.ConfigStore import *
from sap_md_mfg_order_atl_bbl_bbn_bwi_hmd_mbp_mrs_p01_svs.udfs.UDFs import *

def AFKO(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.AFKO")
