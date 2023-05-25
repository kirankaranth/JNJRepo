from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from sap_md_delv_line_bba_bbl_bbn.config.ConfigStore import *
from sap_md_delv_line_bba_bbl_bbn.udfs.UDFs import *

def SELECT_VBAK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.select(col("VBELN"), col("BUKRS_VF"))
