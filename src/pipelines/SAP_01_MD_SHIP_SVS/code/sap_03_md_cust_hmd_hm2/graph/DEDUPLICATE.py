from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_03_md_cust_hmd_hm2.config.ConfigStore import *
from sap_03_md_cust_hmd_hm2.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.dropDuplicates(in0.columns)
