from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_valut.config.ConfigStore import *
from jde_md_matl_valut.udfs.UDFs import *

def SELECT(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("_deleted_"), col("IMITM"), col("IMLITM"), col("IMUOM1"), col("IMGLPT"))
