from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_loc_sjd.config.ConfigStore import *
from jde_md_matl_loc_sjd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_loc_sjd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4102 = JDE_F4102(spark)
    df_JDE_F4102 = collectMetrics(
        spark, 
        df_JDE_F4102, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_NEW_FIELDS_PK = NEW_FIELDS_PK(spark, df_JDE_F4102)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_NEW_FIELDS_PK)
    df_Deduplicate_1 = Deduplicate_1(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_Deduplicate_1 = collectMetrics(
        spark, 
        df_Deduplicate_1, 
        "graph", 
        "dZKH6aWf2424PSiYi_wgb$$qimixgR1yTRGYfCyF2GlA", 
        "6gtATz_1Gqbb5Y51pXdlM$$qr3ScLoXc6TMXE6531rFS"
    )
    MD_MATL_LOC(spark, df_Deduplicate_1)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_LOC_SJD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_LOC_SJD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
