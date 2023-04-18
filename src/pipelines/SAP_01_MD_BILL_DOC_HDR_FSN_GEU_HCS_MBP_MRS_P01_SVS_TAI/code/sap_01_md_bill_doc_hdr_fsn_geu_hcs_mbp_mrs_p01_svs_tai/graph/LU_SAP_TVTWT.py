from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *

def LU_SAP_TVTWT(spark: SparkSession, in0: DataFrame):
    keyColumns = ['''VTWEG''']
    valueColumns = ['''VTEXT''']
    createLookup("LU_SAP_TVTWT", in0, spark, keyColumns, valueColumns)
