from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line.config.ConfigStore import *
from sap_01_md_sls_ordr_line.udfs.UDFs import *

def LU_SAP_TVM5T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MVGR5''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVM5T", in0, spark, keyColumns, valueColumns)
