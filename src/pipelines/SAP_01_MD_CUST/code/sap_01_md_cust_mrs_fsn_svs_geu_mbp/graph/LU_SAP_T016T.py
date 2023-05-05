from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.udfs.UDFs import *

def LU_SAP_T016T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''brsch''']
    valueColumns = ['''brtxt''']
    createLookup("LU_SAP_T016T", in0, spark, keyColumns, valueColumns)
