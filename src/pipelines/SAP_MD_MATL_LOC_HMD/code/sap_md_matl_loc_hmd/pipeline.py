from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_loc_hmd.config.ConfigStore import *
from sap_md_matl_loc_hmd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_T141T = T141T(spark)
    df_T141T = collectMetrics(
        spark, 
        df_T141T, 
        "graph", 
        "W4Zb51eOckNI8rDy_COsa$$UAFMWvm--Uok-yANDYMyO", 
        "xvxWTr2fc96Koy5R-t9OX$$eV4sz2DchD6Qou8NecMoK"
    )
    df_T024 = T024(spark)
    df_T024 = collectMetrics(
        spark, 
        df_T024, 
        "graph", 
        "MEigmgDEkjncakuqwjKOZ$$xk_iApxbWt3dVpIZU5nCF", 
        "DgigLVH08kNpILPiawmrh$$0fV_nZP8hiqkob4lpCWZk"
    )
    df_T024D = T024D(spark)
    df_T024D = collectMetrics(
        spark, 
        df_T024D, 
        "graph", 
        "S0QrJTrvE9UFvcKZQWSIu$$gAs40tNrHxpmR2kbM_l6h", 
        "jYtSiH8tP-UR37csYVyQQ$$i0w6fh4foMQyqr4PkNqxI"
    )
    df_MANDT_FILTER_2 = MANDT_FILTER_2(spark, df_T024D)
    df_NSDM_V_MARC = NSDM_V_MARC(spark)
    df_NSDM_V_MARC = collectMetrics(
        spark, 
        df_NSDM_V_MARC, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_NSDM_V_MARC)
    df_MANDT_FILTER_1 = MANDT_FILTER_1(spark, df_T141T)
    df_Join_1 = Join_1(spark, df_MANDT_FILTER, df_MANDT_FILTER_1)
    df_Join_2 = Join_2(spark, df_Join_1, df_MANDT_FILTER_2)
    df_T024F = T024F(spark)
    df_T024F = collectMetrics(
        spark, 
        df_T024F, 
        "graph", 
        "nUyicwWPN_W1LJq5-ITib$$3lDW4fF6ysdfPzqcmFlsr", 
        "QoZNJ5Z6hPFlRKYW12TAN$$fUJxTG0e7gEVf5WlBSDg-"
    )
    df_MANDT_FILTER_3 = MANDT_FILTER_3(spark, df_T024F)
    df_Join_3 = Join_3(spark, df_Join_2, df_MANDT_FILTER_3)
    df_MANDT_FILTER_4 = MANDT_FILTER_4(spark, df_T024)
    df_Join_4 = Join_4(spark, df_Join_3, df_MANDT_FILTER_4)
    df_NEW_FIELDS_PK = NEW_FIELDS_PK(spark, df_Join_4)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_PK)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zqgwxFtlSFlDyzhEbnTk6$$d-94H6BgyeU15erzMnsDT", 
        "NnAGYtZnDQCWq1rBcnPym$$K9nn3cuIq6Mwyd4w4i5w2"
    )
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    MetricsCollector.initializeMetrics(spark)
    spark.conf.set("prophecy.collect.basic.stats", "true")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    spark.conf.set("spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.ColumnPruning")
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_LOC_HMD")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_LOC_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
