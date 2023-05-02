from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_p01.config.ConfigStore import *
from sap_01_md_sls_ordr_line_p01.udfs.UDFs import *

def LU_SAP_TVM4T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MVGR4''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVM4T", in0, spark, keyColumns, valueColumns)
