from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_bom_hdr_bw2.config.ConfigStore import *
from jde_md_bom_hdr_bw2.udfs.UDFs import *

def JDE_F3002_ADT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.DBNAME}.{Config.TBNAME} WHERE _deleted_ = 'F'")
