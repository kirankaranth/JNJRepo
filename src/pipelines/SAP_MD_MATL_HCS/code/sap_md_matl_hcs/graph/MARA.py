from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hcs.config.ConfigStore import *
from sap_md_matl_hcs.udfs.UDFs import *

def MARA(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"hcs.mara")
