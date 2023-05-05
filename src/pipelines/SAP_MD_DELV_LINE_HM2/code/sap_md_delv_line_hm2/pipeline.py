from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_delv_line_hm2.config.ConfigStore import *
from sap_md_delv_line_hm2.udfs.UDFs import *
from prophecy.utils import *
from sap_md_delv_line_hm2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_DS_SAP_01_LIPS = DS_SAP_01_LIPS(spark)
    df_DS_SAP_01_LIPS = collectMetrics(
        spark, 
        df_DS_SAP_01_LIPS, 
        "graph", 
        "E50SbQFN6GfOzQ5aRIKjY$$tarrW2ZyGO0rD1myENoGr", 
        "zgoRvX-4JP_s5gRThii8o$$D_3A8qjT5GQUWuyXpmdZC"
    )
    df_MANDT_FILTER_LIPS = MANDT_FILTER_LIPS(spark, df_DS_SAP_01_LIPS)
    df_DS_SAP_01_LIKP = DS_SAP_01_LIKP(spark)
    df_DS_SAP_01_LIKP = collectMetrics(
        spark, 
        df_DS_SAP_01_LIKP, 
        "graph", 
        "aXZoJnwk2xWctredKpgMn$$vC1KlpmHrafHbfz8sCSxZ", 
        "JdA0Y5ZzUAHyCKTRuxBK3$$DXSL5k2jxL8ADOhDa7yYY"
    )
    df_MANDT_FILTER_LIKP = MANDT_FILTER_LIKP(spark, df_DS_SAP_01_LIKP)
    df_DS_SAP_01_VBAK = DS_SAP_01_VBAK(spark)
    df_DS_SAP_01_VBAK = collectMetrics(
        spark, 
        df_DS_SAP_01_VBAK, 
        "graph", 
        "8bs8c4MYBaiecnucCfXZN$$Q5oa9Q99EduUrdL_sneZL", 
        "FhTgT0MO6VpAFkL0AfNaF$$XMm06gCwts059SXHuc36M"
    )
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_DS_SAP_01_VBAK)
    df_DS_SAP_01_VBAP = DS_SAP_01_VBAP(spark)
    df_DS_SAP_01_VBAP = collectMetrics(
        spark, 
        df_DS_SAP_01_VBAP, 
        "graph", 
        "oSabIfC1bR5S88xOAIVJz$$Jo-_BN0pfsvPkySWTWDT-", 
        "GNRsBHSzrzpg-V2twZyld$$pOp7bwaS-S33nZRxTTB1k"
    )
    df_MANDT_FILTER_VBAP = MANDT_FILTER_VBAP(spark, df_DS_SAP_01_VBAP)
    df_DS_SAP_01_TVM4T = DS_SAP_01_TVM4T(spark)
    df_DS_SAP_01_TVM4T = collectMetrics(
        spark, 
        df_DS_SAP_01_TVM4T, 
        "graph", 
        "ieADm--EV3pUHIch5WizP$$WJ-wMrjZHnhxQGPDSyin4", 
        "w8rDDiI5FGJ6_aZgW4dPz$$VORU4atEbkJfVW3Hdgtwy"
    )
    df_MANDT_FILTER_TVM4T = MANDT_FILTER_TVM4T(spark, df_DS_SAP_01_TVM4T)
    df_Join_1 = Join_1(
        spark, 
        df_MANDT_FILTER_LIPS, 
        df_MANDT_FILTER_LIKP, 
        df_MANDT_FILTER_VBAK, 
        df_MANDT_FILTER_VBAP, 
        df_MANDT_FILTER_TVM4T
    )
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "dGVP4TCgWySSYk_aDRxH6$$txnVMsngaBpA0r0OxhFvn", 
        "7pSMsRC4GMnP8rzbyDI0f$$rAoshd1rJ9H7HgTTMhOaw"
    )
    MD_DELV_LINE(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "UThVtuhtHzrvbMIHn2YyQ$$jo6R1GV08GKsLeIQw5tzf", 
        "uUbp0d3iFX5kP8Qv4dtpg$$YmjY7PNjk5jaUAT31K3r0"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_DELV_LINE_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_DELV_LINE_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
