from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sls_doc_ptnr_func_hmd.config.ConfigStore import *
from sap_md_sls_doc_ptnr_func_hmd.udfs.UDFs import *
from prophecy.utils import *
from sap_md_sls_doc_ptnr_func_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VBPA = SAP_VBPA(spark)
    df_SAP_VBPA = collectMetrics(
        spark, 
        df_SAP_VBPA, 
        "graph", 
        "a30GjZyB1z1LxHdydcwty$$KDZnRjVaJe2ljhf2lSaVA", 
        "Yf4nSPbBUCeGnx9U-7UqC$$W4SNjK0-qnieq1nZAZ3M8"
    )
    df_SAP_VBAK = SAP_VBAK(spark)
    df_SAP_VBAK = collectMetrics(
        spark, 
        df_SAP_VBAK, 
        "graph", 
        "0ca8JuTNzBqUFrzqNuUfL$$NcltRhbpOxr1uzyv3UgnC", 
        "KL-eBStI5ar--t1WHYkAw$$3kBPCjQuFXEQZqWP_G73B"
    )
    df_MANDT_VBAK = MANDT_VBAK(spark, df_SAP_VBAK)
    df_MANDT_VBPA = MANDT_VBPA(spark, df_SAP_VBPA)
    df_Join_1 = Join_1(spark, df_MANDT_VBPA, df_MANDT_VBAK)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELDS_ORDER_REFORMAT = SET_FIELDS_ORDER_REFORMAT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELDS_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELDS_ORDER_REFORMAT, 
        "graph", 
        "WqbUNLqskS7YVOuw092cD$$ubdkZG05CbUjelHuklFBV", 
        "rGerJmy0lraZOL5TfXXuI$$PbtLCGnGfZQB11MfDHYx5"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELDS_ORDER_REFORMAT)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_FILTER, 
        "graph", 
        "KRlShnpF9hsFnJmUdfrmD$$mq76TGlLsBVtoWN_epUQq", 
        "hrLCWvONfBVuMJB45CGQf$$JP9VyW0h9U4YFqfuzn9-2"
    )
    df_DUPLICATE_FILTER.cache().count()
    df_DUPLICATE_FILTER.unpersist()
    MD_SLS_DOC_PTNR_FUNC(spark, df_SET_FIELDS_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_SLS_DOC_PTNR_FUNC_HM2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_SLS_DOC_PTNR_FUNC_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
