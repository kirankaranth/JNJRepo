from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sap_md_sup_prchsng_org.config.ConfigStore import *
from sap_md_sup_prchsng_org.udfs.UDFs import *
from prophecy.utils import *
from sap_md_sup_prchsng_org.graph import *

def pipeline(spark: SparkSession) -> None:
    df_SAP_LFM1 = SAP_LFM1(spark)
    df_SAP_LFM1 = collectMetrics(
        spark, 
        df_SAP_LFM1, 
        "graph", 
        "kV8uRWTXojbSCYZ-YvAhf$$Gbp8FLwIbIkAHi0U6Evqi", 
        "a9PV-Aufscs-wySt1O072$$3-PBpLFyL5g1D0E72VkOx"
    )
    df_MANDT_FILTER = MANDT_FILTER(spark, df_SAP_LFM1)
    df_NEW_FIELDS = NEW_FIELDS(spark, df_MANDT_FILTER)
    df_FIELDS_ORDER = FIELDS_ORDER(spark, df_NEW_FIELDS)
    df_FIELDS_ORDER = collectMetrics(
        spark, 
        df_FIELDS_ORDER, 
        "graph", 
        "GMTB6wcc_wMMN9DGVFOfv$$VoQ_FncjSOruKAbywYxNW", 
        "Q3rCscDXO34ZawQAMoGfd$$vt4ntwhN1Q9JT0coFs1Pn"
    )
    MD_SUP_PRCHSNG_ORG(spark, df_FIELDS_ORDER)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/SAP_MD_SUP_PRCHSNG_ORG")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/SAP_MD_SUP_PRCHSNG_ORG")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
