from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_doc_itm_incm_invc_hm2.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2.udfs.UDFs import *

def MD_DOC_ITM_INVC(spark: SparkSession, in0: DataFrame):
    in0.write\
        .format("delta")\
        .option("replaceWhere", f"SRC_SYS_CD = '{Config.sourceSystem}'")\
        .mode("overwrite")\
        .saveAsTable(f"{Config.targetSchema}.MD_DOC_ITM_INCM_INVC")
