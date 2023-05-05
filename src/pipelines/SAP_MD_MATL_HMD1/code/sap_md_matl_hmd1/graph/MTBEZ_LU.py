from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def MTBEZ_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MTART''']
    valueColumns = ['''MTBEZ''']
    createLookup("MTBEZ_LU", in0, spark, keyColumns, valueColumns)
