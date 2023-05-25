from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_02_md_plnt_mtr.config.ConfigStore import *
from jde_02_md_plnt_mtr.udfs.UDFs import *

def DUPLICATE_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
