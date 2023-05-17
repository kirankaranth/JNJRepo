from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bbl.config.ConfigStore import *
from sap_md_matl_bbl.udfs.UDFs import *

def MAT_SPEC_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MAT_NUM''']
    valueColumns = ['''ATWRT''']
    createLookup("MAT_SPEC_LU", in0, spark, keyColumns, valueColumns)
