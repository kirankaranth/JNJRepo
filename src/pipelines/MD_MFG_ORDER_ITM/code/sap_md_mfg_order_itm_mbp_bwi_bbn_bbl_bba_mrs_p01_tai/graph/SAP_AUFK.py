from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.config.ConfigStore import *
from sap_md_mfg_order_itm_mbp_bwi_bbn_bbl_bba_mrs_p01_tai.udfs.UDFs import *

def SAP_AUFK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.aufk")