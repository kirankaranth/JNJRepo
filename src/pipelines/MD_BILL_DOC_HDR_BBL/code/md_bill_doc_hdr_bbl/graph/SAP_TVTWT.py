from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from md_bill_doc_hdr_bbl.config.ConfigStore import *
from md_bill_doc_hdr_bbl.udfs.UDFs import *

def SAP_TVTWT(spark: SparkSession) -> DataFrame:
    return spark.sql(f"SELECT * FROM {Config.sourceDatabase}.TVTWT WHERE _deleted_ = 'F' and SPRAS = 'E'")
