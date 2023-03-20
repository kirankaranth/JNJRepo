from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_inv.config.ConfigStore import *
from jde_01_md_matl_inv.udfs.UDFs import *

def JDE_F41021_2(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM gmd.f41021 WHERE _deleted_ = 'F'")
