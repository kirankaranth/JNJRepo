from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_inv_hm2.config.ConfigStore import *
from sap_01_md_matl_inv_hm2.udfs.UDFs import *

def DS_SAP_03_NSDM_V_MSKU(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.nsdm_v_msku")
