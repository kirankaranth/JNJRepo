from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_hcs.config.ConfigStore import *
from sap_01_md_sls_ordr_hcs.udfs.UDFs import *

def LU_SAP_TVV1T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''KVGR1''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVV1T", in0, spark, keyColumns, valueColumns)
