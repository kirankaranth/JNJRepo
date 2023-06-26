from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.config.ConfigStore import *
from jde_md_sls_doc_hier_gmd_bw2_deu_djd_jem_jet_jsw_mtr_sjd_jes.udfs.UDFs import *

def DUPLICATE_CHECK_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
