from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_jet.config.ConfigStore import *
from jde_md_matl_loc_jet.udfs.UDFs import *

def JDE_F4102_ADT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.{Config.sourceTable} WHERE _deleted_ = 'F'")
