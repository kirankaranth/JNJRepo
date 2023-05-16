from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd1.config.ConfigStore import *
from sap_md_matl_hmd1.udfs.UDFs import *

def MAT_TYPE_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MAT_NUM''']
    valueColumns = ['''ATWRT''']
    createLookup("MAT_TYPE_LU", in0, spark, keyColumns, valueColumns)
