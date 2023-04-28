from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_inv_hcs.config.ConfigStore import *
from sap_md_matl_inv_hcs.udfs.UDFs import *

def DS_SAP_0003_MSKU(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.msku")
