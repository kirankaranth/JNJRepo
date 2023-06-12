from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_bbl.config.ConfigStore import *
from sap_md_matl_bbl.udfs.UDFs import *

def MAKTX_LU(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MATNR''']
    valueColumns = ['''MAKTX''']
    createLookup("LU_MAKT_MAKTX", in0, spark, keyColumns, valueColumns)
