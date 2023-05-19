from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_dstn_chn_bwi.config.ConfigStore import *
from sap_md_matl_dstn_chn_bwi.udfs.UDFs import *

def LU_SAP_t179t(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''PRODH''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_t179t", in0, spark, keyColumns, valueColumns)
