from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def SELECT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("_deleted_"), col("IMITM"), col("IMLITM"), col("IMUOM1"), col("IMGLPT"))
