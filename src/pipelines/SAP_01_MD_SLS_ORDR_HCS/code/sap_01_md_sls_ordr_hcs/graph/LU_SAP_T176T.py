from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_hcs.config.ConfigStore import *
from sap_01_md_sls_ordr_hcs.udfs.UDFs import *

def LU_SAP_T176T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''BSARK''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_T176T", in0, spark, keyColumns, valueColumns)
