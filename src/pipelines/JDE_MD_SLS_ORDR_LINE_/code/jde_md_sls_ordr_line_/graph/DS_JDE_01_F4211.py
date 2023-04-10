from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_sls_ordr_line_.config.ConfigStore import *
from jde_md_sls_ordr_line_.udfs.UDFs import *

def DS_JDE_01_F4211(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.DBNAME}.{Config.DBTABLE1} WHERE _deleted_ = 'F'")
