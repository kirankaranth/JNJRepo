from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.udfs.UDFs import *

def LU_SAP_TVAST(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''AUFSP''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVAST", in0, spark, keyColumns, valueColumns)
