from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.config.ConfigStore import *
from sap_md_matl_uom_mbp_svs_p01_hcs_hmd.udfs.UDFs import *

def DS_SAP_01_T006(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.{Config.DBTABLE1}")
