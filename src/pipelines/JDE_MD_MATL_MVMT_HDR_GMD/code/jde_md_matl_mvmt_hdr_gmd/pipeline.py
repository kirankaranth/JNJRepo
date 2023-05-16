from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from jde_md_matl_mvmt_hdr_gmd.config.ConfigStore import *
from jde_md_matl_mvmt_hdr_gmd.udfs.UDFs import *
from prophecy.utils import *
from jde_md_matl_mvmt_hdr_gmd.graph import *

def pipeline(spark: SparkSession) -> None:
    df_F0005 = F0005(spark)
    df_F4111 = F4111(spark)
    df_FILTER = FILTER(spark, df_F0005)
    df_FILTER_DELETED = FILTER_DELETED(spark, df_F4111)
    df_Join_1 = Join_1(spark, df_FILTER_DELETED, df_FILTER)
    df_NEW_FIELDS_RENAME_FORMAT = NEW_FIELDS_RENAME_FORMAT(spark, df_Join_1)
    df_SET_FIELDS_OUTPUT = SET_FIELDS_OUTPUT(spark, df_NEW_FIELDS_RENAME_FORMAT)
    MD_MATL_MVMT_HDR(spark, df_SET_FIELDS_OUTPUT)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/JDE_MD_MATL_MVMT_HDR_GMD")
    
    MetricsCollector.start(spark = spark, pipelineId = "pipelines/JDE_MD_MATL_MVMT_HDR_GMD")
    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
