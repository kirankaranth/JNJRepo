from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *

def Filter_5(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter(lit(True))
