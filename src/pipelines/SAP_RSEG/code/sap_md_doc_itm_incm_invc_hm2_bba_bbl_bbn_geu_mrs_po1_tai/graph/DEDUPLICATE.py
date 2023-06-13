from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.config.ConfigStore import *
from sap_md_doc_itm_incm_invc_hm2_bba_bbl_bbn_geu_mrs_po1_tai.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window.partitionBy("SRC_SYS_CD", "ACTG_DOC_NUM", "FISC_YR", "DOC_ITM_IN_INVC_DOC").orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
