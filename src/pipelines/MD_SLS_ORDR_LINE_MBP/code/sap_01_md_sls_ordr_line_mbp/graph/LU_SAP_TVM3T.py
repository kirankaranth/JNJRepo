from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_mbp.config.ConfigStore import *
from sap_01_md_sls_ordr_line_mbp.udfs.UDFs import *

def LU_SAP_TVM3T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MVGR3''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVM3T", in0, spark, keyColumns, valueColumns)
