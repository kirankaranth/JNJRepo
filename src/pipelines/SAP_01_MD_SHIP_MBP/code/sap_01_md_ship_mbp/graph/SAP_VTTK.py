from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ship_mbp.config.ConfigStore import *
from sap_01_md_ship_mbp.udfs.UDFs import *

def SAP_VTTK(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.vttk")
