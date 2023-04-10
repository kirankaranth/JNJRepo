from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bom.config.ConfigStore import *
from sap_md_matl_bom.udfs.UDFs import *

def SAP_MAST(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"atl.mast")
