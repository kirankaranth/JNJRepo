from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai.config.ConfigStore import *
from sap_01_md_bill_doc_hdr_fsn_geu_hcs_mbp_mrs_p01_svs_tai.udfs.UDFs import *

def MD_BILL_DOC_HDR(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_BILL_DOC_HDR")
