from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def MATL_T_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''DRKY''']
    valueColumns = ['''DRDL01''']
    createLookup("MATL_T_LU", in0, spark, keyColumns, valueColumns)
