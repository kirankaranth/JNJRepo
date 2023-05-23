from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_tai.config.ConfigStore import *
from sap_md_matl_inv_tai.udfs.UDFs import *

def DS_SAP_000001_MARD(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mard")
