from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def SQL(spark: SparkSession, in0: DataFrame) -> (DataFrame):
    in0.createOrReplaceTempView("in0")
    df1 = spark.sql(
        "SELECT COMCU, COITM, Count(*) Records FROM in0 GROUP BY COMCU, COITM HAVING Records >1\r\n\r\n--SELECT LIITM, LIMCU, Count(*) Records FROM in0 GROUP BY LIITM, LIMCU HAVING Records >1"
    )

    return df1
