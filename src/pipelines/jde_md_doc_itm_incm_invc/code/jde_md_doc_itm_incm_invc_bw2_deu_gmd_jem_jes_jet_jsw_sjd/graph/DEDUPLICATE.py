from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *

def DEDUPLICATE(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0\
        .withColumn(
          "row_number",
          row_number()\
            .over(Window.partitionBy("SRC_SYS_CD", "ACTG_DOC_NUM", "FISC_YR", "DOC_ITM_IN_INVC_DOC").orderBy(lit(1)))
        )\
        .filter(col("row_number") == lit(1))\
        .drop("row_number")
