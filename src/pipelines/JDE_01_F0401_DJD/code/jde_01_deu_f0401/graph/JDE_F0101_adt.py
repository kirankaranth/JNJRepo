from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_deu_f0401.config.ConfigStore import *
from jde_01_deu_f0401.udfs.UDFs import *

def JDE_F0101_adt(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.F0101_adt")
