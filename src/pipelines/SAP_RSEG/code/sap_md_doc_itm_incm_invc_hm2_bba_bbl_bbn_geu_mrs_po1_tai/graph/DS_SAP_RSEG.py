from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.udfs.UDFs import *

def DS_SAP_RSEG(spark: SparkSession) -> DataFrame:
    return spark.read.table(f"{Config.sourceDatabase}.rseg")