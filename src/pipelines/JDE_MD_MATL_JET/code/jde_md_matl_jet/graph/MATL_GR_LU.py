from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def MATL_GR_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''DRKY''']
    valueColumns = ['''DRDL01''']
    createLookup("MATL_GR_LU", in0, spark, keyColumns, valueColumns)
