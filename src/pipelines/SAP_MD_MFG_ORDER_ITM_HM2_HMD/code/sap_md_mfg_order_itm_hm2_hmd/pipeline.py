from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_mfg_order_itm_hm2_hmd.config.ConfigStore import *
from sap_md_mfg_order_itm_hm2_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_mfg_order_itm_hm2_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_AFPO = SAP_AFPO(spark)
    df_SAP_AFPO = collectMetrics(
        spark, 
        df_SAP_AFPO, 
        "graph", 
        "XoVg9YvpbGC6FjbIyNMCl$$C7sqGjbO1414VT5JfB5tY", 
        "-LHcmU0DZwTvfep_Ikad3$$z88y7Z5NQ_buolfy4th9C"
    )
    df_MANDT_FILTER_AFPO = MANDT_FILTER_AFPO(spark, df_SAP_AFPO)
    df_SAP_AUFK = SAP_AUFK(spark)
    df_SAP_AUFK = collectMetrics(
        spark, 
        df_SAP_AUFK, 
        "graph", 
        "IHvO7Dhhh3DTReQEenosW$$CqIDa8P_grB4X_-9ZC0x0", 
        "y8VlwreSVtkfYMFVTfCfS$$e0EOknrjPwRmGXlsAbjQT"
    )
    df_MANDT_FILTER_AUFK = MANDT_FILTER_AUFK(spark, df_SAP_AUFK)
    df_SELECT_AUFK = SELECT_AUFK(spark, df_MANDT_FILTER_AUFK)
    df_JOIN = JOIN(spark, df_MANDT_FILTER_AFPO, df_SELECT_AUFK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_JOIN)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "P8NYObEPq_4S_bT4LkRZ4$$0dsT0wR5EutDM_tV9rlQT", 
        "n1dXODwFFf2LvRYrPTdcr$$zLwCALsw3eoALKtdm2ib0"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "vuUV1VNquucCB7iOXxq8I$$0WjHX3mtDJOo9Oy7EumMx", 
        "QzAzW4y7ku2DjwM86QlLs$$80EWPzONXGoh-eQ8WQxRZ"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    MD_MFG_ORDER_ITM(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MFG_ORDER_ITM_HM2_HMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MFG_ORDER_ITM_HM2_HMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
