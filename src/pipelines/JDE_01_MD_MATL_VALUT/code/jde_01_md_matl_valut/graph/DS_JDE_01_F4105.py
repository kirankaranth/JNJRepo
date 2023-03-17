from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_matl_valut.config.ConfigStore import *
from jde_01_md_matl_valut.udfs.UDFs import *

def DS_JDE_01_F4105(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM bw2.f4105_adt WHERE _deleted_ = 'F' and COLEDG = '07'")
