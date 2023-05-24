from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_bw2_md_matl.config.ConfigStore import *
from jde_bw2_md_matl.udfs.UDFs import *

def SLD_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''IVLITM''']
    valueColumns = ['''min_shelf_life_in_days''']
    createLookup("SLD_LU", in0, spark, keyColumns, valueColumns)
