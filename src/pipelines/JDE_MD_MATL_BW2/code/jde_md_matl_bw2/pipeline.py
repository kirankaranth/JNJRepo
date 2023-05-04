from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_bw2.config.ConfigStore import *
from jde_md_matl_bw2.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_bw2.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4102_ADT = JDE_F4102_ADT(spark)
    df_JDE_F4102_ADT = collectMetrics(
        spark, 
        df_JDE_F4102_ADT, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_NEW_FIELDS_PK = NEW_FIELDS_PK(spark, df_JDE_F4102_ADT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_PK)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zqgwxFtlSFlDyzhEbnTk6$$d-94H6BgyeU15erzMnsDT", 
        "NnAGYtZnDQCWq1rBcnPym$$K9nn3cuIq6Mwyd4w4i5w2"
    )
    MD_MATL_LOC(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_BW2")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_BW2")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
