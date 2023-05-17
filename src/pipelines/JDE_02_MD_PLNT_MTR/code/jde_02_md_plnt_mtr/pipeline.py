from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_02_md_plnt_mtr.config.ConfigStore import *
from jde_02_md_plnt_mtr.udfs.UDFs import *
from prophecy.utils import *
from jde_02_md_plnt_mtr.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F0116 = JDE_F0116(spark)
    df_JDE_F0116 = collectMetrics(
        spark, 
        df_JDE_F0116, 
        "graph", 
        "4zljP0FlXXHBtlHtSH5R2$$MkDcVRnqDK5IfGcFMBrzx", 
        "bbTi6qu675vn_0fgQWzax$$71yjJlLh4hCRHrX5yvhh6"
    )
    df_JDE_F0116_FILTER = JDE_F0116_FILTER(spark, df_JDE_F0116)
    df_JDE_F0006 = JDE_F0006(spark)
    df_JDE_F0006 = collectMetrics(
        spark, 
        df_JDE_F0006, 
        "graph", 
        "kM0H6L5KyBQBDPpjS0zqi$$8mrN0V8fO8ALksoSk7U6s", 
        "21DF_bl72RV0BeadNutJh$$xoub2cfLSrdOtci7enhJm"
    )
    df_JDE_F0006_FILTER = JDE_F0006_FILTER(spark, df_JDE_F0006)
    df_Join_JDE = Join_JDE(spark, df_JDE_F0006_FILTER, df_JDE_F0116_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_JDE)
    df_DEDUPLICATE = DEDUPLICATE(spark, df_NEW_FIELDS_RENAME_FORMAT)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DEDUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zl4casZt-Vg54MMvBB8aq$$8My__SKf3yxPM13DHH9Bq", 
        "D6aGbLrGF7EnuUOoEBVTQ$$VBxaof6J2VdKgoSaz0QKZ"
    )
    df_DUPLICATE_CHECK = DUPLICATE_CHECK(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUPLICATE_FILTER = DUPLICATE_FILTER(spark, df_DUPLICATE_CHECK)
    df_DUPLICATE_FILTER = collectMetrics(
        spark, 
        df_DUPLICATE_FILTER, 
        "graph", 
        "uu_tAy54SIVeSCqnWscb7$$t8axTYMppa7ANTQTVF9Yl", 
        "jhwVdsoRS-78-Eog1xleI$$Sw2KPKV9NcXS5yzQFiMP0"
    )
    df_DUPLICATE_FILTER.cache().count()
    df_DUPLICATE_FILTER.unpersist()
    MD_PLNT(spark, df_SET_FIELD_ORDER_REFORMAT)

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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_02_MD_PLNT_MTR")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_02_MD_PLNT_MTR")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
