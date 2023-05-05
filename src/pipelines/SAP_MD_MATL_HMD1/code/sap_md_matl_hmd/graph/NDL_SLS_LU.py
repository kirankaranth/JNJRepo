from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_hmd.config.ConfigStore import *
from sap_md_matl_hmd.udfs.UDFs import *

def NDL_SLS_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MAT_NUM''']
    valueColumns = ['''ATWRT''']
    createLookup("NDL_SLS_LU", in0, spark, keyColumns, valueColumns)
