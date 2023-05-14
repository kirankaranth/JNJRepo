from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def F4105_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''COITM''', '''COMCU''']
    valueColumns = ['''COUNCS''']
    createLookup("F4105_LU", in0, spark, keyColumns, valueColumns)
