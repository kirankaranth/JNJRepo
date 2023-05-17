from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from jde_md_matl_jet.config.ConfigStore import *
from jde_md_matl_jet.udfs.UDFs import *

def MAT_SPEC_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = []
    valueColumns = []
    createLookup("", in0, spark, keyColumns, valueColumns)
