from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_line_geu.config.ConfigStore import *
from sap_01_md_sls_ordr_line_geu.udfs.UDFs import *

def LU_SAP_TVM2T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MVGR2''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVM2T", in0, spark, keyColumns, valueColumns)
