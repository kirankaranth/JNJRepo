from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_01_md_ship_mtk.config.ConfigStore import *
from jde_01_md_ship_mtk.udfs.UDFs import *

def JDE_F4211_SD(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.targetSchema}.MD_SHIP WHERE _deleted_ = 'F'")
