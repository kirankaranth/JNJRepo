from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def FRAN_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''DRKY''']
    valueColumns = ['''DRDL01''']
    createLookup("FRAN_LU", in0, spark, keyColumns, valueColumns)
