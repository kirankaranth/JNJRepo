from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *

def DUPLICATE_CHECK_FILTER(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.filter((col("PK_COUNT") > lit(1)))
