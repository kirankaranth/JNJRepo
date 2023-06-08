from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_sls_ordr_bbn_geu_p01_mbp_svs.config.ConfigStore import *
from sap_01_md_sls_ordr_bbn_geu_p01_mbp_svs.udfs.UDFs import *

def LU_SAP_TVV4T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''KVGR4''']
    valueColumns = ['''BEZEI''']
    createLookup("LU_SAP_TVV4T", in0, spark, keyColumns, valueColumns)
