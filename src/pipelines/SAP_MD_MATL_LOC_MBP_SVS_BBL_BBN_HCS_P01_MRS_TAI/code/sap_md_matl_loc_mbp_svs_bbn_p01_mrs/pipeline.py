from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.config.ConfigStore import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_matl_loc_mbp_svs_bbn_p01_mrs.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_T141T = SAP_T141T(spark)
    df_SAP_T141T = collectMetrics(
        spark, 
        df_SAP_T141T, 
        "graph", 
        "ch1EJ8FCq9pW3x5LmD8qN$$_wgE8xjVxNP45lp2LdP1E", 
        "oP7PgmGR-vKZx4XIxebLg$$CYGd6ZBxnoQrpIwAAJ3zI"
    )
    df_MANDT_FILTER_T141T = MANDT_FILTER_T141T(spark, df_SAP_T141T)
    LU_SAP_T141T(spark, df_MANDT_FILTER_T141T)
    df_SAP_T024 = SAP_T024(spark)
    df_SAP_T024 = collectMetrics(
        spark, 
        df_SAP_T024, 
        "graph", 
        "J5F4eq8F1JpepVwzgJl3I$$gVTObIxcs9CEbtfaAuqHP", 
        "IRE41XdUeoGWU309c13F4$$GN_04QWQg8oYD6c2ATbdR"
    )
    df_MANDT_FILTER_T024 = MANDT_FILTER_T024(spark, df_SAP_T024)
    LU_SAP_T024(spark, df_MANDT_FILTER_T024)
    df_SAP_T460A = SAP_T460A(spark)
    df_SAP_T460A = collectMetrics(
        spark, 
        df_SAP_T460A, 
        "graph", 
        "qanaR_et-lRJ8GxCfKvPx$$2I2Z5Aqi8maQNaw41D5oz", 
        "T-E0q-KqWOUJ13ap65-O9$$vasxfzK5jGPq1semo3myz"
    )
    df_MANDT_FILTER_T141T_1 = MANDT_FILTER_T141T_1(spark, df_SAP_T460A)
    LU_SAP_T460A(spark, df_MANDT_FILTER_T141T_1)
    df_SAP_T024D = SAP_T024D(spark)
    df_SAP_T024D = collectMetrics(
        spark, 
        df_SAP_T024D, 
        "graph", 
        "tBIQA_jzW1fAbrMaRWpZt$$ZTBXG3j7kZZiJ7R7_oAnV", 
        "bhjXbHnNGyzB4Hj7xuXzf$$T87m5qtf3_haWocKwsUYh"
    )
    df_MANDT_FILTER_T024D = MANDT_FILTER_T024D(spark, df_SAP_T024D)
    LU_SAP_T024D(spark, df_MANDT_FILTER_T024D)
    df_SAP_MARC = SAP_MARC(spark)
    df_SAP_MARC = collectMetrics(
        spark, 
        df_SAP_MARC, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_MARC)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zqgwxFtlSFlDyzhEbnTk6$$d-94H6BgyeU15erzMnsDT", 
        "NnAGYtZnDQCWq1rBcnPym$$K9nn3cuIq6Mwyd4w4i5w2"
    )
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "TVv0GKNUPd41ARM9nuJhE$$vQaQ14kJueMlVpK65vFQR", 
        "dpcBG3i1S7KjjvCqoRANO$$yRJJ2MJfHwBHUJPE_n3jC"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_LOC_MBP_SVS_BBL_BBN_HCS_P01_MRS_TAI")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_LOC_MBP_SVS_BBL_BBN_HCS_P01_MRS_TAI")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
