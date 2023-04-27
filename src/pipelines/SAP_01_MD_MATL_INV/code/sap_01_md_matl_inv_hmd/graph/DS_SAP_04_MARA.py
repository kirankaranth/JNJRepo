from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_inv_hmd.config.ConfigStore import *
from sap_01_md_matl_inv_hmd.udfs.UDFs import *

def DS_SAP_04_MARA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.mara")
