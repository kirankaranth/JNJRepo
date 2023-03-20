from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_loc.config.ConfigStore import *
from sap_01_md_matl_loc.udfs.UDFs import *

def LU_T141T_EN(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MMSTA''']
    valueColumns = ['''MTSTB''']
    createLookup("LU_T141T_EN", in0, spark, keyColumns, valueColumns)
