from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.config.ConfigStore import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_cust_mrs_fsn_svs_geu_mbp.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVAST = SAP_TVAST(spark)
    df_SAP_TVAST = collectMetrics(
        spark, 
        df_SAP_TVAST, 
        "graph", 
        "XgEzlEqV_RMID8gJ7waCJ$$GM-_06sUHUG_0rhT2tTVO", 
        "B_Cb7apxtE6rwyKKSF-GM$$SjB4kUU2vwX4KSJRls9Oy"
    )
    df_MANDT_FILTER_TVAST = MANDT_FILTER_TVAST(spark, df_SAP_TVAST)
    LU_SAP_TVAST(spark, df_MANDT_FILTER_TVAST)
    df_SAP_T016T = SAP_T016T(spark)
    df_SAP_T016T = collectMetrics(
        spark, 
        df_SAP_T016T, 
        "graph", 
        "zmL-OpPa7iIE5UfRGIGus$$cZAiLXQPafn28YJf_lQf0", 
        "HiUZcFFg12zA5Q2HOFw2D$$kqlLP5bWLvcyRksbFb4yn"
    )
    df_MANDT_FILTER_T016T = MANDT_FILTER_T016T(spark, df_SAP_T016T)
    LU_SAP_T016T(spark, df_MANDT_FILTER_T016T)
    df_DS_01_SAP_KNA1 = DS_01_SAP_KNA1(spark)
    df_DS_01_SAP_KNA1 = collectMetrics(
        spark, 
        df_DS_01_SAP_KNA1, 
        "graph", 
        "7FiguLGhkLkTMjjbuS_-r$$t3i0bcHt7YxTn8u695cVL", 
        "oy_tucL4l59wb13X7ZbqH$$tgBNwLNg-6jZh73to-w_R"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_01_SAP_KNA1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "5rh_SiJ3gg41Zr8bZxQ-c$$X_tD_PrfC1vGKZm7PS9i1", 
        "qwgBUWieqOjTj0_pvmxEY$$sb4OxzWZFV18wT_lmBv2Z"
    )
    MD_CUST(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_CUST")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_CUST")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
