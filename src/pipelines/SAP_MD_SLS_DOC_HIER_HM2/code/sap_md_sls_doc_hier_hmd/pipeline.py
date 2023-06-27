from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sls_doc_hier_hmd.config.ConfigStore import *
from sap_md_sls_doc_hier_hmd.udfs.UDFs import *
from prophecy.utils import *
from prophecy.transpiler import call_spark_fcn
from prophecy.transpiler.fixed_file_schema import *
from sap_md_sls_doc_hier_hmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_VBAK = SAP_VBAK(spark)
    df_SAP_VBAK = collectMetrics(
        spark, 
        df_SAP_VBAK, 
        "graph", 
        "7KsOVQdaFGGhL04nJm_7M$$jHntF7k3J0NgnIft4ntFf", 
        "ryPuUh9xQZ1-25yFkfYR3$$OIKUkFLV1tKFgDZWa4bOk"
    )
    df_MANDT_FILTER_VBAK = MANDT_FILTER_VBAK(spark, df_SAP_VBAK)
    df_FIELDS_VBAK = FIELDS_VBAK(spark, df_MANDT_FILTER_VBAK)
    df_SAP_VBFA = SAP_VBFA(spark)
    df_SAP_VBFA = collectMetrics(
        spark, 
        df_SAP_VBFA, 
        "graph", 
        "XXhfvXl14f1Pa1nBmB75u$$hbNpJYswOs91t49Qb5aXk", 
        "O8R52T_arZJ-EDJS6dIrF$$Zikrt9R3RwPnpNxWagj67"
    )
    df_MANDT_FILTER_VBFA = MANDT_FILTER_VBFA(spark, df_SAP_VBFA)
    df_JOIN_SAP = JOIN_SAP(spark, df_FIELDS_VBAK, df_MANDT_FILTER_VBFA)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_JOIN_SAP)
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_NEW_FIELDS)
    df_DUPLICATE_CHECK_FILTER = DUPLICATE_CHECK_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_CHECK_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_CHECK_FILTER, 
        "graph", 
        "IL27EDW9qj8fR4O9Q4p4M$$fYiYCJmLY4TDx9d4qW66a", 
        "96CaQqIebTJBxip1jvRmH$$CW0kRVC1nFXNcfrI5-dzl"
    )
    df_DUPLICATE_CHECK_FILTER.cache().count()
    df_DUPLICATE_CHECK_FILTER.unpersist()
    df_FIELD_ORDER = FIELD_ORDER(spark, df_NEW_FIELDS)
    df_FIELD_ORDER = collectMetrics(
        spark, 
        df_FIELD_ORDER, 
        "graph", 
        "lFky-UqAPDBCGt26xYCWN$$U-wYUx5SVBlAbrgWGKGeo", 
        "Xcn3vKgMy7zO4dJn3jKaj$$YjDDrweO1Q4LlO6-2t1v7"
    )
    MD_SLS_DOC_HIER(spark, df_FIELD_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_SLS_DOC_HIER_HM2")
    registerUDFs(spark)
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_SLS_DOC_HIER_HM2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
