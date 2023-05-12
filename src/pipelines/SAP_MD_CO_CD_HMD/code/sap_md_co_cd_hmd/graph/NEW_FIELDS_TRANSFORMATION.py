from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_co_cd_hmd.config.ConfigStore import *
from sap_md_co_cd_hmd.udfs.UDFs import *

def NEW_FIELDS_TRANSFORMATION(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0
