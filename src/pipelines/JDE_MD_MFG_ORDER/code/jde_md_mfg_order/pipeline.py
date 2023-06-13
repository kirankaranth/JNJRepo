from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_mfg_order.config.ConfigStore import *
from jde_md_mfg_order.udfs.UDFs import *
from prophecy.utils import *
from jde_md_mfg_order.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F4801 = F4801(spark)
    df_F4801 = collectMetrics(
        spark, 
        df_F4801, 
        "graph", 
        "hwnl0gNY7Su2qn85O1qQT$$JxX3PAoeRV15LX5-YbsLw", 
        "bmXKmjjwb_vqV1CFB--w6$$DmnnZYdqyjTk-aMBW7Ak6"
    )
    df_DEL = DEL(spark, df_F4801)
    df_XFORM = XFORM(spark, df_DEL)
    df_SELECT_FIELDS = SELECT_FIELDS(spark, df_XFORM)
    df_SELECT_FIELDS = collectMetrics(
        spark, 
        df_SELECT_FIELDS, 
        "graph", 
        "Xi6nZZ9qYgG-uExR5Hd1i$$xgMBDgkYJ2xOf7s5BtKQo", 
        "H9g9_Jcg04MCGNKiHGj57$$yo8dEF6gNSdDiA6sSShdd"
    )
    MD_MFG_ORDER(spark, df_SELECT_FIELDS)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MFG_ORDER")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MFG_ORDER")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
