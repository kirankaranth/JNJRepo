from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_itm_hm2_hmd.config.ConfigStore import *
from sap_md_mfg_order_itm_hm2_hmd.udfs.UDFs import *

def SAP_AUFK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.aufk")
