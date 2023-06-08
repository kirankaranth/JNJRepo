from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_tai.config.ConfigStore import *
from sap_md_matl_inv_tai.udfs.UDFs import *

def DS_SAP_000003_MARA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.mara")
