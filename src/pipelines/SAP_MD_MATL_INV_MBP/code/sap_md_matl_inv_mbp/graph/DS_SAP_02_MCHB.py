from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_mbp.config.ConfigStore import *
from sap_md_matl_inv_mbp.udfs.UDFs import *

def DS_SAP_02_MCHB(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mchb")
