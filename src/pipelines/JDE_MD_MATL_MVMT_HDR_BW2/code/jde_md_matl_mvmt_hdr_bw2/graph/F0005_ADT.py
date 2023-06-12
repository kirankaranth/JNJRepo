from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_mvmt_hdr_bw2.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_bw2.udfs.UDFs import *

def F0005_ADT(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.f0005_adt")
