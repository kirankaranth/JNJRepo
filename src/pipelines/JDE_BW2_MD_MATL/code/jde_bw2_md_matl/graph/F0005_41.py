from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def F0005_41(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.{Config.DBTABLE1} WHERE Trim(DRSY) = '41'")
