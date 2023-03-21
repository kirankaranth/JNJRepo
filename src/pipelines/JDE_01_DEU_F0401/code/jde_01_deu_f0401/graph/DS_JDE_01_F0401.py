from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def DS_JDE_01_F0401(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM deu.f0401 WHERE _deleted_ = 'F' ")
