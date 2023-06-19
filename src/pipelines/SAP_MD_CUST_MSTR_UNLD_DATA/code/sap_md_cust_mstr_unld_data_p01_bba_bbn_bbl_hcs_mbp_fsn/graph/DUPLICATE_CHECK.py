from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.config.ConfigStore import *
from sap_md_cust_mstr_unld_data_p01_bba_bbn_bbl_hcs_mbp_fsn.udfs.UDFs import *

def DUPLICATE_CHECK(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("SRC_SYS_CD"), col("CUST_NUM"), col("UNLOADING_PT"))

    return df1.agg(count(lit(1)).alias("PK_COUNT"))
