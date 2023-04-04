from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def JDE_F0401(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.F0401 WHERE _deleted_ = 'F' ")
