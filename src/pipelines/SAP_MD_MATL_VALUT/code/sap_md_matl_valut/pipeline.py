from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_matl_valut.config.ConfigStore import *
from sap_md_matl_valut.udfs.UDFs import *
from prophecy.utils import *
from sap_md_matl_valut.graph import *

def pipeline(spark: SparkSession) -> None:
    df_MARA = MARA(spark)
    df_MARA = collectMetrics(
        spark, 
        df_MARA, 
        "graph", 
        "F3MxQaAjYc3Eag_mZmxgz$$tXn31e6OcvrVlyRMMvQQl", 
        "vLLgPrGLVgofG19Ua7cjT$$oK4VLViQgv730cD1jUiA-"
    )
    df_MANDT = MANDT(spark, df_MARA)
    MEINS_LU(spark, df_MANDT)
    df_MBEW = MBEW(spark)
    df_MBEW = collectMetrics(
        spark, 
        df_MBEW, 
        "graph", 
        "g11XZNZU8hnQG_g-0N-sT$$ypU9VzDaAVojALDRg2iiT", 
        "5hEVOWv12SnANv2G5J7xy$$LvwxcMQ3xoX_Nv1k_0c-G"
    )
    df_MANDT_1 = MANDT_1(spark, df_MBEW)
    df_XFORM = XFORM(spark, df_MANDT_1)
    df_SELECT = SELECT(spark, df_XFORM)
    df_SELECT = collectMetrics(
        spark, 
        df_SELECT, 
        "graph", 
        "_fydvOhi9WzMz188AKRVP$$TO78TrnTmzfzSxayG8RKu", 
        "kWUYrd8vMCU6p9ttsJ8zG$$1pVJ4zgKkeKzI0WMRb4Nd"
    )
    TARGET(spark, df_SELECT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_MATL_VALUT")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_MATL_VALUT")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
