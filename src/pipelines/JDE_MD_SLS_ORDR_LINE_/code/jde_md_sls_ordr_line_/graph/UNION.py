from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_.config.ConfigStore import *
from jde_md_sls_ordr_line_.udfs.UDFs import *

def UNION(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.unionAll(in1)
