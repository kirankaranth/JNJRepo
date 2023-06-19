from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_matl_loc_hmd.config.ConfigStore import *
from sap_01_md_matl_loc_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_matl_loc_hmd.graph import *

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
        "LRGvTORAdE1Uh9pMOi8Qg$$Jv1pMeaWrxMZB2zDa_dqq", 
        "M1reoTiNNJIFcv5Jm3MyH$$HHmjTGdvTIrg5x0msv3BL"
    )
    df_MANDT_FILTER_T460A = MANDT_FILTER_T460A(spark, df_SAP_T460A)
    LU_SAP_T460A(spark, df_MANDT_FILTER_T460A)
    df_SAP_T024F = SAP_T024F(spark)
    df_SAP_T024F = collectMetrics(
        spark, 
        df_SAP_T024F, 
        "graph", 
        "o69aPorRGd-9MSBFlF3nj$$fjHufGZQ_J17KJAaEywhY", 
        "Pck87WAlGc_z0OZSbz5pt$$26foLlDSq6hsa-lvn_7g5"
    )
    df_MANDT_FILTER_T024F = MANDT_FILTER_T024F(spark, df_SAP_T024F)
    LU_SAP_T024F(spark, df_MANDT_FILTER_T024F)
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
    df_SAP_NSDM_V_MARC = SAP_NSDM_V_MARC(spark)
    df_SAP_NSDM_V_MARC = collectMetrics(
        spark, 
        df_SAP_NSDM_V_MARC, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_NSDM_V_MARC)
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_MATL_LOC_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_MATL_LOC_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
