from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def INV_SUM(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("LIITM"), col("LIMCU"), col("LIPBIN"))

    return df1.agg(
        sum(col("LIPQOH")).cast(DecimalType(18, 4)).alias("TOT_INV"), 
        sum(col("LIQTIN")).cast(DecimalType(18, 4)).alias("INSP")
    )
