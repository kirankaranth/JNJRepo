from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_matl_valut_jde.config.ConfigStore import *
from md_matl_valut_jde.udfs.UDFs import *

def LITM_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''IMITM''']
    valueColumns = ['''IMLITM''']
    createLookup("LITM_LU", in0, spark, keyColumns, valueColumns)
