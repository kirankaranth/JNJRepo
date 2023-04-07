from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def GET_DUP(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("_pk_"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))