from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl.config.ConfigStore import *
from jde_md_matl.udfs.UDFs import *

def F0005_41(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM gmd.f0005 WHERE Trim(DRSY) = '41'")
