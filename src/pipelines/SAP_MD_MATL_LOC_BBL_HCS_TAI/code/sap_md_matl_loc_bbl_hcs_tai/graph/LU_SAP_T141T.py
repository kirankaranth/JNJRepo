from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_matl_loc_bbl_hcs_tai.config.ConfigStore import *
from sap_md_matl_loc_bbl_hcs_tai.udfs.UDFs import *

def LU_SAP_T141T(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''MMSTA''']
    valueColumns = ['''MTSTB''']
    createLookup("LU_SAP_T141T", in0, spark, keyColumns, valueColumns)
