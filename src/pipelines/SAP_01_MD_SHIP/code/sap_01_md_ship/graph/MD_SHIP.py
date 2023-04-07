from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_ship.config.ConfigStore import *
from sap_01_md_ship.udfs.UDFs import *

def MD_SHIP(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("error").saveAsTable(f"dev_md_l1.md_ship")
