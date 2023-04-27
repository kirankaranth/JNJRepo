from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_loc_deu.config.ConfigStore import *
from jde_md_matl_loc_deu.udfs.UDFs import *

def JDE_F4102(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceSystem}.{Config.sourceTable} WHERE _deleted_='F'")
