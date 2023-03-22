from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def JDE_F4102(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM jes.f4102_adt WHERE _deleted_ = 'F'")
