from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.config.ConfigStore import *
from jde_md_doc_itm_incm_invc_bw2_deu_gmd_jem_jes_jet_jsw_sjd.udfs.UDFs import *

def FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("_deleted_") == lit("F")))
