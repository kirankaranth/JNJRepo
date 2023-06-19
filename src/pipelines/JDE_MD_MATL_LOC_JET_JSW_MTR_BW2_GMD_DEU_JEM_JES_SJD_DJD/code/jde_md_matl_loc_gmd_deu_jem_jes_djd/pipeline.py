from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.config.ConfigStore import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_loc_gmd_deu_jem_jes_djd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_JDE_F4102 = JDE_F4102(spark)
    df_JDE_F4102 = collectMetrics(
        spark, 
        df_JDE_F4102, 
        "graph", 
        "J4_uS3Q4GhLzOZu0ywART$$NUBQymIdgcgIKQgmw18UQ", 
        "XKQAzwCyxKqPPiRqgbCB_$$oQrRD8nAiVmZB6zBLPCWx"
    )
    df_JDE_F4102_FILTER = JDE_F4102_FILTER(spark, df_JDE_F4102)
    df_NEW_FIELDS_TRANSFORMATION = NEW_FIELDS_TRANSFORMATION(spark, df_JDE_F4102_FILTER)
    df_DUPLICATE = DUPLICATE(spark, df_NEW_FIELDS_TRANSFORMATION)
    df_SET_FIELD_ORDER_REFORMAT = SET_FIELD_ORDER_REFORMAT(spark, df_DUPLICATE)
    df_SET_FIELD_ORDER_REFORMAT = collectMetrics(
        spark, 
        df_SET_FIELD_ORDER_REFORMAT, 
        "graph", 
        "zqgwxFtlSFlDyzhEbnTk6$$d-94H6BgyeU15erzMnsDT", 
        "NnAGYtZnDQCWq1rBcnPym$$K9nn3cuIq6Mwyd4w4i5w2"
    )
    df_GET_DUP = GET_DUP(spark, df_SET_FIELD_ORDER_REFORMAT)
    df_DUP_FILTER = DUP_FILTER(spark, df_GET_DUP)
    df_DUP_FILTER = collectMetrics(
        spark, 
        df_DUP_FILTER, 
        "graph", 
        "tJbB3Et6-T73M0IAn21Ri$$K5hOcNE8jppnliCBLUtcZ", 
        "VFGQTK8mmpdRwWk2K7b44$$fb6zN7utDpfdW5K-A-g3K"
    )
    df_DUP_FILTER.cache().count()
    df_DUP_FILTER.unpersist()
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
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_LOC_JET_JSW_MTR_BW2_GMD_DEU_JEM_JES_SJD_DJD")
    
    MetricsCollector.start(
        spark = spark,
        pipelineId = "pipelines/JDE_MD_MATL_LOC_JET_JSW_MTR_BW2_GMD_DEU_JEM_JES_SJD_DJD"
    )
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
