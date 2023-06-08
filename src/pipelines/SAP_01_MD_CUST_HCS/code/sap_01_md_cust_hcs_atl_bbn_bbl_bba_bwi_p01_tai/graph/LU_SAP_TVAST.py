from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.config.ConfigStore import *
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.udfs.UDFs import *

def LU_SAP_TVAST(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''AUFSP''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVAST", in0, spark, keyColumns, valueColumns)
