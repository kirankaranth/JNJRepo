from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.config.ConfigStore import *
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.udfs.UDFs import *
from prophecy.utils import *
from sap_01_md_cust_hcs_atl_bbn_bbl_bba_bwi_p01_tai.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_TVAST = SAP_TVAST(spark)
    df_SAP_TVAST = collectMetrics(
        spark, 
        df_SAP_TVAST, 
        "graph", 
        "Fjd09XJ0xiNyWNiKcuHm5$$jWAXEWiP6sUMqFusIPazQ", 
        "VpRH7bNjH33k7oL6cB6T8$$AJWhEU84-oz9U-34U0qxB"
    )
    df_MANDT_FILTER_TVAST = MANDT_FILTER_TVAST(spark, df_SAP_TVAST)
    LU_SAP_TVAST(spark, df_MANDT_FILTER_TVAST)
    df_DS_SAP_02_KNA1 = DS_SAP_02_KNA1(spark)
    df_DS_SAP_02_KNA1 = collectMetrics(
        spark, 
        df_DS_SAP_02_KNA1, 
        "graph", 
        "bOs3pnYQ3EMecjOltVe6b$$Y6bNtMzDawJ8WnnMcWBvW", 
        "XP54Y7tYL77kluNpNgQ2v$$xT4pF-MB4PCQ4vStpkGID"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_DS_SAP_02_KNA1)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_MANDT_FILTER)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "-F-RV5TnHAiWAsstPW-MX$$WjYrnEUN8bpREK_4Wpcfb", 
        "45PgXKwr6xzOen5RJL9B5$$muRQNuSGwtEhRMPd2saTD"
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_01_MD_CUST_HCS")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_01_MD_CUST_HCS")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
