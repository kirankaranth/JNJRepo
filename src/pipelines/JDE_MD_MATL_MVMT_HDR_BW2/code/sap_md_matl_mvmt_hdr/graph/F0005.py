from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_mvmt_hdr.config.ConfigStore import *
from sap_md_matl_mvmt_hdr.udfs.UDFs import *

def F0005(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceSystem}.f0005_adt")
