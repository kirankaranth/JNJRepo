from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_plnt.config.ConfigStore import *
from sap_01_md_plnt.udfs.UDFs import *

def LU_SAP_T005T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''LAND1''']
    valueColumns = ['''LANDX''']
    createLookup("LU_SAP_T005T", in0, spark, keyColumns, valueColumns)
