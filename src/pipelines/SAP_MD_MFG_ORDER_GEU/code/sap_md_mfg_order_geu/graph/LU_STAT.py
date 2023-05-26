from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_mfg_order_geu.config.ConfigStore import *
from sap_md_mfg_order_geu.udfs.UDFs import *

def LU_STAT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''OBJNR''']
    valueColumns = ['''STAT''', '''TXT04''']
    createLookup("LU_STAT", in0, spark, keyColumns, valueColumns)
