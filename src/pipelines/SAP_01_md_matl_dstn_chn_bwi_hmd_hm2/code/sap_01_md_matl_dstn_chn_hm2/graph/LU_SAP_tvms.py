from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_matl_dstn_chn_hm2.config.ConfigStore import *
from sap_01_md_matl_dstn_chn_hm2.udfs.UDFs import *

def LU_SAP_tvms(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''VMSTA''']
    valueColumns = ['''SPVBC''']
    createLookup("LU_SAP_tvms", in0, spark, keyColumns, valueColumns)
